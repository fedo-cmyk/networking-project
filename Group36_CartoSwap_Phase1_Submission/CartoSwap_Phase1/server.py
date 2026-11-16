import argparse
import socket
import time
from pathlib import Path

import protocol as p


HOST = '0.0.0.0'
NETWORK_OVERHEAD = 28
RATE_BYTES_PER_SEC = 5000


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--port', type=int, default=p.SERVER_PORT)
    parser.add_argument('--log-dir', type=Path, default=Path('logs'))
    args = parser.parse_args()

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

        next_send_time = (
            max(sent_at, next_send_time)
            + total_size / RATE_BYTES_PER_SEC
        )

        return len(packet)

    try:
        while True:
            data, source = sock.recvfrom(2048)

            log.log('recv', data, source)

            msg = p.unpack(data)

            if msg is None:
                print(f'Invalid packet from {source}', flush=True)
                continue

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

                known = [
                    (pid, ip, port)
                    for pid, (ip, port) in peers.items()
                    if pid != peer_id
                ]

                send(
                    p.PEER_LIST,
                    p.peer_list_payload(peer_id, known),
                    source
                )

                print(
                    f'INIT from {address} assigned peer {peer_id}',
                    flush=True
                )

            elif msg.type == p.CHUNK_REQ:
                if len(msg.payload) != 2:
                    continue

                if source not in ids_by_address:
                    print(
                        f'Unregistered request from {source}',
                        flush=True
                    )
                    continue

                chunk_id = p.parse_chunk_id(msg.payload)

                if not 0 <= chunk_id < p.NUM_CHUNKS:
                    continue

                if chunk_id not in p.slice_of(ids_by_address[source]):
                    print(
                        f'Chunk {chunk_id} outside assigned slice for {source}',
                        flush=True
                    )
                    continue

                payload = p.chunk_data_payload(
                    chunk_id,
                    p.make_chunk(chunk_id)
                )

                send(p.CHUNK_DATA, payload, source)

                print(
                    f'Sent chunk {chunk_id} to {source}',
                    flush=True
                )

            else:
                print(
                    f'Ignoring unsupported message {p.type_name(msg.type)}',
                    flush=True
                )

    except KeyboardInterrupt:
        print('\nServer stopped', flush=True)

    finally:
        log.close()
        sock.close()


if __name__ == '__main__':
    main()
