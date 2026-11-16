"""server.py - el tracker w el seeder el batee2 le CartoSwap/1 (Phase 1).

El server beyesma3 3ala UDP port 9000. el client byeb3at INIT fa el server beyedeeh raqmo
w el peers el ma3rofeen (PEER_LIST). ba3dein el client byeb3at CHUNK_REQ fa el server
byrod b CHUNK_DATA, bas lel chunks elly fel slice bta3to, w b sor3a mat3adda 50 kbps.
"""
import argparse
import socket
import time
from pathlib import Path

import protocol as p

HOST = '0.0.0.0'
NETWORK_OVERHEAD = 28          # IP (20) + UDP (8) byet7esbo fel sor3a 3ashan el 50 kbps tefdal sa7ee7a
RATE_BYTES_PER_SEC = 6000      # 48 kbps, a2al shewaya men el 50 kbps (6250 byte/sanya)


# ===== el server =====
# main: byefta7 el socket, w loop wa7da betestaqbel el packets w betrod 3aleha.
# send(): byb3at packet w beyestanna el wa2t el mounaseb 3ashan el sor3a matkhodsh aktar men el limit.
def main(argv=None):
    args = arguments(argv)
    args.log_dir.mkdir(parents=True, exist_ok=True)
    log = p.PacketLog(str(args.log_dir / 'server_packets.csv'))
    peers = {}
    ids_by_address = {}
    next_id = 1
    seq = 0
    next_send_time = time.monotonic()
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((HOST, args.port))
    print(f'Server listening on UDP port {args.port}', flush=True)

    def send(kind, payload, destination, flags=0):
        nonlocal seq, next_send_time
        seq += 1
        packet = p.pack(kind, seq, p.SERVER_ID, payload, flags)
        total_size = len(packet) + NETWORK_OVERHEAD
        now = time.monotonic()
        if now < next_send_time:
            time.sleep(next_send_time - now)
        sock.sendto(packet, destination)
        sent_at = time.monotonic()
        log.log('send', packet, destination)
        next_send_time = max(sent_at, next_send_time) + total_size / args.rate
        return len(packet)

    try:
        while True:
            try:
                data, source = sock.recvfrom(2048)
            except ConnectionResetError:
                continue          # Windows: client 2afal el socket bta3o, fa negtahel el error w nkamel
            log.log('recv', data, source)
            msg = p.unpack(data)
            if msg is None:
                print(f'Invalid packet from {source}', flush=True)
                continue

            # INIT: client gedeed. benedeeh raqm (1 le 4) w nerg3lo PEER_LIST.
            if msg.type == p.INIT:
                if len(msg.payload) != 2:
                    continue
                listen_port = p.parse_init(msg.payload)
                address = (source[0], listen_port)
                if address not in ids_by_address:
                    if next_id > p.NUM_CLIENTS:
                        print('Client limit reached', flush=True)
                        continue
                    ids_by_address[address] = next_id
                    next_id += 1
                peer_id = ids_by_address[address]
                peers[peer_id] = address
                known = [(pid, ip, port) for pid, (ip, port) in peers.items()
                         if pid != peer_id]
                send(p.PEER_LIST, p.peer_list_payload(peer_id, known), source)
                print(f'INIT from {address} assigned peer {peer_id}', flush=True)

            # CHUNK_REQ: client 3ayez chunk. benb3atlo CHUNK_DATA law el chunk fel slice bta3to.
            elif msg.type == p.CHUNK_REQ:
                if len(msg.payload) != 2:
                    continue
                if source not in ids_by_address:
                    print(f'Unregistered request from {source}', flush=True)
                    continue
                chunk_id = p.parse_chunk_id(msg.payload)
                if not 0 <= chunk_id < p.NUM_CHUNKS:
                    continue
                if chunk_id not in p.slice_of(ids_by_address[source]):
                    print(f'Chunk {chunk_id} outside assigned slice for {source}', flush=True)
                    continue
                payload = p.chunk_data_payload(chunk_id, p.make_chunk(chunk_id))
                send(p.CHUNK_DATA, payload, source)
                print(f'Sent chunk {chunk_id} to {source}', flush=True)

            else:
                print(f'Ignoring unsupported message {p.type_name(msg.type)}', flush=True)
    except KeyboardInterrupt:
        print('\nServer stopped', flush=True)
    finally:
        log.close()
        sock.close()


# ===== el arguments =====
# --port: port el server, --rate: el sor3a b byte/sanya (law 3ayez tegarrab), --log-dir: makan el log.
def arguments(argv=None):
    limit = p.SERVER_RATE_KBPS * 1000 // 8
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--port', type=int, default=p.SERVER_PORT)
    parser.add_argument('--rate', type=float, default=RATE_BYTES_PER_SEC,
                        help=f'max bytes/second incl. {NETWORK_OVERHEAD} B IP+UDP overhead (limit {limit})')
    parser.add_argument('--log-dir', type=Path, default=Path('logs'))
    args = parser.parse_args(argv)
    if not 1 <= args.port <= 65535:
        parser.error('port must be between 1 and 65535')
    if not 0 < args.rate <= limit:
        parser.error(f'--rate must be between 1 and {limit} bytes/second (50 kbps)')
    return args


if __name__ == '__main__':
    main()
