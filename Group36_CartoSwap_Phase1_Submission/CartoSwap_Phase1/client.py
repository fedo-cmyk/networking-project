
import argparse
import hashlib
import json
import select
import socket
import sys
import time
from pathlib import Path

import protocol as p



# Phase1Client beyeHfaz kol 7aga el client yehtagha: el socket, el peers, el chunks elly wasalet,
# el requests el mestanniya (pending), w el statistics.
class Phase1Client:
    def __init__(self, args):
        self.args = args
        self.server = (socket.gethostbyname(args.server_host), args.server_port)
        self.listen_port = args.listen_port or 5000 + args.number
        self.peer_id = 0
        self.peers = []
        self.seq = 0
        self.have = {}
        self.pending = {}
        self.targets = []
        self.started = None
        self.deadline = None
        self.stats = dict(sent_packets=0, received_packets=0, invalid_packets=0,
                          duplicate_chunk_count=0, retransmissions=0)
        args.log_dir.mkdir(parents=True, exist_ok=True)
        args.output_dir.mkdir(parents=True, exist_ok=True)
        self.log = p.PacketLog(str(args.log_dir / f'client{args.number}_packets.csv'))
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            self.sock.bind((args.bind_host, self.listen_port))
            self.sock.setblocking(False)
        except Exception:
            self.sock.close()
            self.log.close()
            raise

    def close(self):
        self.sock.close()
        self.log.close()

    # ===== el ersal w el esteqbal =====
    # send: beyb3at packet lel server. receive_until: beyestanna resala sa7eeha le7ad deadline,
    # w beyetgahel ay packet mesh men el server aw mesh sa7ee7 men gher ma yeb3d el wa2t.
    def send(self, kind, payload, retransmission=False):
        self.seq = (self.seq + 1) & 0xFFFFFFFF
        flags = p.FLAG_RETX if retransmission else 0
        packet = p.pack(kind, self.seq, self.peer_id, payload, flags)
        if len(packet) > p.MAX_DATAGRAM:
            raise ValueError(f'application datagram exceeds {p.MAX_DATAGRAM} bytes')
        self.sock.sendto(packet, self.server)
        self.log.log('send', packet, self.server)
        self.stats['sent_packets'] += 1
        if retransmission:
            self.stats['retransmissions'] += 1

    def receive_until(self, deadline):
        while True:
            now = time.monotonic()
            if now >= self.deadline:
                raise TimeoutError('overall run deadline exceeded')
            remaining = min(deadline, self.deadline) - now
            if remaining <= 0:
                return None
            ready, _, _ = select.select([self.sock], [], [], remaining)
            if not ready:
                return None
            try:
                raw, address = self.sock.recvfrom(65535)
            except (BlockingIOError, ConnectionResetError):
                continue          # ConnectionResetError: Windows beyeb3atha lamma el server mesh sha8al
            msg = p.unpack(raw)
            valid = (address == self.server and len(raw) <= p.MAX_DATAGRAM and msg is not None
                     and msg.peer_id == p.SERVER_ID)
            self.log.log('recv', raw, address, '' if valid else 'rejected_envelope_or_source')
            self.stats['received_packets'] += 1
            if not valid:
                self.stats['invalid_packets'] += 1
                continue
            return msg

    def reject(self, reason):
        self.stats['invalid_packets'] += 1
        print(f'client {self.args.number}: ignored {reason}', flush=True)


    # beyb3at INIT w beyestanna PEER_LIST. law mawsalsh fi 500 ms beyeb3at tani (le7ad --attempts).
    # el ID elly byegi men el server hwa elly beyeHdded el slice, mesh raqm el command line.
    def join(self):
        for attempt in range(self.args.attempts):
            self.send(p.INIT, p.init_payload(self.listen_port), attempt > 0)
            deadline = time.monotonic() + self.args.timeout
            while True:
                msg = self.receive_until(deadline)
                if msg is None:
                    break
                if msg.type != p.PEER_LIST:
                    self.reject('non-PEER_LIST while joining')
                    continue
                body = msg.payload
                if len(body) < 3 or len(body) != 3 + 8 * body[2]:
                    self.reject('malformed PEER_LIST')
                    continue
                peer_id, peers = p.parse_peer_list(body)
                ids = [item[0] for item in peers]
                if (not 1 <= peer_id <= p.NUM_CLIENTS or
                        len(peers) >= p.NUM_CLIENTS or len(set(ids)) != len(ids) or
                        any(pid == peer_id or not 1 <= pid <= p.NUM_CLIENTS or port == 0
                            for pid, _, port in peers)):
                    self.reject('invalid peer registry')
                    continue
                self.peer_id, self.peers = peer_id, peers
                self.targets = list(p.slice_of(peer_id))[:self.args.chunks]
                print(f'client {self.args.number}: joined as peer {peer_id}; '
                      f'port {self.listen_port}; {len(peers)} known peers; '
                      f'seed chunks {self.targets[0]}..{self.targets[-1]}', flush=True)
                return
        raise TimeoutError(f'no valid PEER_LIST after {self.args.attempts} INIT attempts')

    # ===== el download =====
    # request: beyb3at CHUNK_REQ w beyesagel el deadline bta3o. download: beyb3at le7ad --window
    # requests f nafs el wa2t, w ay request et2akhar aktar men 500 ms beyetb3at tani. el chunk el
    # mawgood aw el corrupt aw el mesh matlob beyetgahel, w el duplicate beyet7seb bas.
    def request(self, cid, attempts):
        self.send(p.CHUNK_REQ, p.chunk_req_payload(cid), attempts > 1)
        self.pending[cid] = (time.monotonic() + self.args.timeout, attempts)

    def download(self):
        next_target = 0
        while len(self.have) < len(self.targets):
            if time.monotonic() >= self.deadline:
                raise TimeoutError('overall run deadline exceeded')
            while next_target < len(self.targets) and len(self.pending) < self.args.window:
                self.request(self.targets[next_target], 1)
                next_target += 1
            now = time.monotonic()
            for cid, (expires, attempts) in list(self.pending.items()):
                if now >= expires:
                    if attempts >= self.args.attempts:
                        raise TimeoutError(f'chunk {cid} missing after {attempts} attempts')
                    self.request(cid, attempts + 1)
            msg = self.receive_until(min(item[0] for item in self.pending.values()))
            if msg is None:
                continue
            if msg.type != p.CHUNK_DATA:
                self.reject('non-CHUNK_DATA while downloading')
                continue
            if len(msg.payload) != 2 + p.CHUNK_SIZE:
                self.reject('CHUNK_DATA length')
                continue
            cid = p.parse_chunk_id(msg.payload)
            chunk = msg.payload[2:]
            if cid not in self.targets or (cid not in self.have and cid not in self.pending):
                self.reject(f'unrequested chunk {cid}')
                continue
            if chunk != p.make_chunk(cid):
                self.reject(f'corrupt chunk {cid}')
                continue
            if cid in self.have:
                self.stats['duplicate_chunk_count'] += 1
                continue
            self.have[cid] = chunk
            del self.pending[cid]

    # ===== el tashghil el kamel =====
    # run: join, download, ba3dein beyegma3 el chunks bel ID (mesh bel wa2t el wasalo feeh) w beyektob
    # el .bin w el summary JSON fel results/.
    def run(self):
        self.started = time.monotonic()
        self.deadline = self.started + self.args.max_seconds
        self.join()
        self.download()
        assembled = b''.join(self.have[cid] for cid in self.targets)
        if len(self.have) != len(self.targets) or len(assembled) != len(self.targets) * p.CHUNK_SIZE:
            raise RuntimeError('incomplete seed slice')
        completion_ms = (time.monotonic() - self.started) * 1000
        stem = f'client{self.args.number}'
        binary_path = self.args.output_dir / f'{stem}_seed.bin'
        binary_path.write_bytes(assembled)
        summary = dict(
            status='ok', scope='phase1_server_seed_slice', client_number=self.args.number,
            peer_id=self.peer_id, server=list(self.server), listen_port=self.listen_port,
            known_peers=self.peers, chunk_ids=self.targets, chunks_received=len(self.have),
            total_map_chunks=p.NUM_CHUNKS, full_map_complete=len(self.have) == p.NUM_CHUNKS,
            bytes_assembled=len(assembled), sha256=hashlib.sha256(assembled).hexdigest(),
            seed_download_time_ms=round(completion_ms, 3), **self.stats)
        summary_path = self.args.output_dir / f'{stem}_summary.json'
        summary_path.write_text(json.dumps(summary, indent=2) + '\n', encoding='utf-8')
        print(f'client {self.args.number}: {len(self.have)}/{len(self.targets)} seed chunks verified '
              f'in {completion_ms:.0f} ms; {self.stats["retransmissions"]} retransmissions; '
              f'{self.stats["duplicate_chunk_count"]} duplicates', flush=True)
        print(f'Saved {binary_path} and {summary_path}', flush=True)
        return summary


# ===== el arguments =====
# el options bta3et el command line (kolohom leha default, bas el raqm el awel matloob).
def arguments(argv=None):
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('number', type=int, choices=range(1, p.NUM_CLIENTS + 1),
                        help='local instance number; default port is 5000 + number')
    parser.add_argument('--server-host', default='127.0.0.1')
    parser.add_argument('--server-port', type=int, default=p.SERVER_PORT)
    parser.add_argument('--bind-host', default='0.0.0.0')
    parser.add_argument('--listen-port', type=int)
    parser.add_argument('--chunks', type=int, default=p.SLICE_SIZE,
                        help='number of chunks from assigned seed slice (default: all 64)')
    parser.add_argument('--window', type=int, default=4, help='concurrent chunk requests (1..8)')
    parser.add_argument('--timeout', type=float, default=p.CHUNK_TTL, help='retry timeout, at most 0.5 s')
    parser.add_argument('--attempts', type=int, default=10, help='maximum sends per request, including first')
    parser.add_argument('--max-seconds', type=float, default=30.0, help='overall network deadline')
    parser.add_argument('--log-dir', type=Path, default=Path('logs'))
    parser.add_argument('--output-dir', type=Path, default=Path('results'))
    args = parser.parse_args(argv)
    if not 1 <= args.server_port <= 65535 or (args.listen_port is not None and not 1 <= args.listen_port <= 65535):
        parser.error('ports must be between 1 and 65535')
    if not 1 <= args.chunks <= p.SLICE_SIZE:
        parser.error(f'--chunks must be between 1 and {p.SLICE_SIZE}; server only seeds your slice')
    if not 1 <= args.window <= 8:
        parser.error('--window must be between 1 and 8')
    if not 0 < args.timeout <= p.CHUNK_TTL:
        parser.error('--timeout must be positive and at most 0.5 seconds')
    if args.attempts < 1 or not 0 < args.max_seconds < float('inf'):
        parser.error('--attempts and --max-seconds must be positive and finite')
    return args


# ===== main =====
# beyeshaghal el client w beyrag3 exit code: 0 = nagah, 1 = fashal, 130 = etwa2af b Ctrl+C.
def main(argv=None):
    args = arguments(argv)
    client = None
    try:
        client = Phase1Client(args)
        client.run()
        return 0
    except (OSError, ValueError, RuntimeError) as exc:
        print(f'client {args.number}: FAILED: {exc}', file=sys.stderr, flush=True)
        return 1
    except KeyboardInterrupt:
        print(f'client {args.number}: interrupted', file=sys.stderr)
        return 130
    finally:
        if client is not None:
            client.close()


if __name__ == '__main__':
    sys.exit(main())
