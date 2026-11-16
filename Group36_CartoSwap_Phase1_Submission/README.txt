CartoSwap/1 - Phase 1   (Group 36, CSE361 Computer Networks, Fall 2026, Project 1: P2P-CASP)

Team:
Razan Mohamed Mahmoud Ahmed Mohamed ElBanna — 24P0001
Fahd Khaled Mohamed Hassan Khataan — 24P0073
Omar Ahmed Mohamed Ali Hanfy Kohla — 24P0086
Ali Mohammad Ali Ibrahim Mandour — 24P0115
Farah Khaled Ahmed Hussein — 24P0123


 demo video link : https://drive.google.com/drive/folders/1d2hcCQL7VD9u9j4G-sEAFeZ5hgW7wVCe?usp=sharing

FILES
  protocol.py     shared header, message types, pack/unpack, chunk generator, CSV packet log
  server.py       UDP tracker + rate-limited seeder (port 9000, 6,000 B/s incl. IP/UDP overhead)
  client.py       client: INIT -> PEER_LIST -> downloads its 64-chunk slice (window 4, 500 ms retry)
  run_baseline.py starts 1 server + 4 clients, checks "< 5 s" and "<= 50 kbps", prints PASS/FAIL
  docs/           Proposal, Mini-RFC (sections 1-3)

REQUIREMENTS
  Python 3.9+ . Standard library only. No pip install. Linux, macOS or Windows.


RUN FROM A TERMINAL
  python run_baseline.py                   one baseline run
  python run_baseline.py --runs 3          three runs
  python run_baseline.py --chunks 8        quick test with 8 chunks per client

RUN BY HAND (5 terminals, from this folder)
  python server.py
  python client.py 1        (then 2, 3, 4 in other terminals)
  Options: server.py --port --log-dir ; client.py --server-host --server-port --window
           --timeout --attempts --max-seconds --chunks --log-dir --output-dir
  For several computers: python client.py 1 --server-host <server IP>

OUTPUT
  logs/runN/server_packets.csv, clientK_packets.csv   every packet with time, type, seq, bytes
  results/runN/clientK_summary.json, clientK_seed.bin  per-client result and assembled slice
  results/baseline_summary.csv, baseline_server.csv    table of all runs (times, retx, duplicates, kbps)
  Expected: every client finishes in under 5 s, server peak under 50 kbps, 0 retransmissions.