import argparse
import csv
import json
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path

import protocol as p

HERE = Path(__file__).resolve().parent
OVERHEAD = 28                 # IP + UDP, nafs el 7esba elly el server bey3melha
LIMIT_KBPS = p.SERVER_RATE_KBPS
LIMIT_MS = 5000               # acceptance criteria: kol el clients yekhlaso fi < 5 sanya


# ===== port fady =====
# free_port: beyes2al el system 3an port UDP fady, 3ashan Windows aw ay barnameg tani mayeb2ash shaghal 3ala 5001-5004.
def free_port():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind(('0.0.0.0', 0))
    port = s.getsockname()[1]
    s.close()
    return port


# ===== tashghil run wa7ed =====
# run_once: beyeshaghal el server w el 4 clients fe folder khas bel run (logs/runN, results/runN),
# beyestanna el clients, w beyqafel el server. beyrag3 el wa2t el kolli bel sanya.
def run_once(run_no, args):
    log_dir = HERE / 'logs' / f'run{run_no}'
    out_dir = HERE / 'results' / f'run{run_no}'
    log_dir.mkdir(parents=True, exist_ok=True)
    out_dir.mkdir(parents=True, exist_ok=True)
    console = log_dir / 'server_console.txt'
    server_out = open(console, 'w', encoding='utf-8')
    server = subprocess.Popen(
        [sys.executable, '-u', str(HERE / 'server.py'), '--port', str(args.port),
         '--log-dir', str(log_dir)], cwd=HERE, stdout=server_out, stderr=subprocess.STDOUT)
    clients = []
    try:
        for _ in range(100):                      # beyestanna el server yefta7 el socket (le7ad 5 sanya)
            if server.poll() is not None:
                raise RuntimeError('server exited early: ' + console.read_text(encoding='utf-8')[-300:])
            if 'listening' in console.read_text(encoding='utf-8'):
                break
            time.sleep(0.05)
        else:
            raise RuntimeError('server did not start')
        started = time.monotonic()
        for n in range(1, p.NUM_CLIENTS + 1):
            f = open(log_dir / f'client{n}_console.txt', 'w', encoding='utf-8')
            proc = subprocess.Popen(
                [sys.executable, '-u', str(HERE / 'client.py'), str(n), '--server-port', str(args.port),
                 '--listen-port', str(free_port()), '--chunks', str(args.chunks), '--log-dir', str(log_dir), '--output-dir', str(out_dir)],
                cwd=HERE, stdout=f, stderr=subprocess.STDOUT)
            clients.append((proc, f))
        for proc, _ in clients:
            try:
                proc.wait(timeout=60)
            except subprocess.TimeoutExpired:
                proc.kill()
        return time.monotonic() - started
    finally:
        for proc, f in clients:
            if proc.poll() is None:
                proc.kill()
            f.close()
        if server.poll() is None:
            if sys.platform != 'win32':
                server.send_signal(signal.SIGINT)     # Linux: Ctrl+C 3ashan el log yetaqfel 3ady
            else:
                server.terminate()
            try:
                server.wait(timeout=3)
            except subprocess.TimeoutExpired:
                server.kill()
        server_out.close()


# ===== el 7esab men el logs =====
# server_stats: men server_packets.csv beyes7seb 3adad el packets w el average w a3la kbps fi ay sanya.
# client_rows: men el summary JSON bta3 kol client beyges el wa2t w el retransmissions w el duplicates.
def server_stats(log_dir):
    sends = []
    with open(log_dir / 'server_packets.csv', newline='', encoding='utf-8') as f:
        for row in csv.DictReader(f):
            if row['dir'] == 'send':
                sends.append((float(row['time_ms']), int(row['bytes']) + OVERHEAD))
    if not sends:
        return dict(packets=0, bytes=0, avg_kbps=0.0, peak_kbps=0.0)
    total = sum(size for _, size in sends)
    span_s = max((sends[-1][0] - sends[0][0]) / 1000.0, 0.001)
    peak, window, left = 0, 0, 0
    for right, (t, size) in enumerate(sends):
        window += size
        while t - sends[left][0] >= 1000.0:
            window -= sends[left][1]
            left += 1
        peak = max(peak, window)
    return dict(packets=len(sends), bytes=total, avg_kbps=total * 8 / span_s / 1000.0,
                peak_kbps=peak * 8 / 1000.0)


def client_rows(out_dir):
    rows = []
    for n in range(1, p.NUM_CLIENTS + 1):
        path = out_dir / f'client{n}_summary.json'
        if not path.exists():
            rows.append(dict(client=n, ok=False))
            continue
        s = json.loads(path.read_text(encoding='utf-8'))
        rows.append(dict(client=n, ok=s['status'] == 'ok', peer_id=s['peer_id'],
                         chunks=s['chunks_received'], completion_ms=s['seed_download_time_ms'],
                         retransmissions=s['retransmissions'],
                         duplicates=s['duplicate_chunk_count'], invalid=s['invalid_packets']))
    return rows


# ===== el main =====
# beyeshaghal el run(s), beyetba3 el gadwal, beyektob results/baseline_summary.csv w baseline_server.csv,
# w beyrag3 exit code 0 law PASS, 1 law FAIL.
def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--runs', type=int, default=1)
    parser.add_argument('--chunks', type=int, default=p.SLICE_SIZE)
    parser.add_argument('--port', type=int, default=p.SERVER_PORT)
    args = parser.parse_args()
    (HERE / 'results').mkdir(exist_ok=True)
    client_csv, server_csv = [], []
    failures = []
    for run_no in range(1, args.runs + 1):
        print(f'=== baseline run {run_no}/{args.runs} ===', flush=True)
        try:
            wall = run_once(run_no, args)
        except RuntimeError as exc:
            failures.append(f'run {run_no}: {exc}')
            print('FAILED:', exc)
            continue
        rows = client_rows(HERE / 'results' / f'run{run_no}')
        stats = server_stats(HERE / 'logs' / f'run{run_no}')
        print(f'{"client":<8}{"peer":<6}{"chunks":<8}{"time ms":<10}{"retx":<6}{"dups":<6}{"result"}')
        for r in rows:
            if r['ok']:
                slow = r['completion_ms'] >= LIMIT_MS
                print(f'{r["client"]:<8}{r["peer_id"]:<6}{r["chunks"]:<8}{r["completion_ms"]:<10.0f}'
                      f'{r["retransmissions"]:<6}{r["duplicates"]:<6}{"TIME LIMIT EXCEEDED" if slow else "PASS"}')
                if slow:
                    failures.append(f'run {run_no}: client {r["client"]} took {r["completion_ms"]:.0f} ms (limit {LIMIT_MS})')
            else:
                print(f'{r["client"]:<8}FAILED (see logs/run{run_no}/client{r["client"]}_console.txt):')
                try:
                    tail = (HERE / 'logs' / f'run{run_no}' / f'client{r["client"]}_console.txt').read_text(encoding='utf-8')[-400:]
                except OSError:
                    tail = '(no console file)'
                print('   ' + tail.strip().replace('\n', '\n   '))
                failures.append(f'run {run_no}: client {r["client"]} did not finish')
            client_csv.append(dict(run=run_no, **r))
        print(f'server: {stats["packets"]} packets, average {stats["avg_kbps"]:.1f} kbps, '
              f'peak in any 1 s window {stats["peak_kbps"]:.1f} kbps (limit {LIMIT_KBPS}, overhead included)')
        if stats['peak_kbps'] > LIMIT_KBPS:
            failures.append(f'run {run_no}: server peaked at {stats["peak_kbps"]:.1f} kbps')
        server_csv.append(dict(run=run_no, wall_s=round(wall, 3), **{k: round(v, 3) for k, v in stats.items()}))
    if client_csv:
        keys = ['run', 'client', 'ok', 'peer_id', 'chunks', 'completion_ms', 'retransmissions', 'duplicates', 'invalid']
        with open(HERE / 'results' / 'baseline_summary.csv', 'w', newline='', encoding='utf-8') as f:
            w = csv.DictWriter(f, fieldnames=keys, extrasaction='ignore')
            w.writeheader()
            w.writerows(client_csv)
    if server_csv:
        with open(HERE / 'results' / 'baseline_server.csv', 'w', newline='', encoding='utf-8') as f:
            w = csv.DictWriter(f, fieldnames=list(server_csv[0].keys()))
            w.writeheader()
            w.writerows(server_csv)
    print()
    if failures:
        print('BASELINE FAIL')
        for item in failures:
            print(' -', item)
        return 1
    print('BASELINE PASS: All four clients successfully received their assigned chunks in under 5 seconds, and server bandwidth remained below 50 kbps.')
    print('(Phase 1: Initial server seeding only, with 64 chunks per client. Full peer-to-peer map distribution will be implemented in Phase 2.)')
    return 0


if __name__ == '__main__':
    sys.exit(main())
