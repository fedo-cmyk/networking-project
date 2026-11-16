"""protocol.py - el file el moshtarak ben server.py w client.py (CartoSwap/1).

Gowwah: shakl el header (el envelope 12 byte), pack/unpack, el payload helpers,
el chunks el sabta, w el PacketLog. El server w el client byestakhdemo nafs el
file 3ashan yeb2o 7afzeen nafs el kawa3ed (ghalta wa7da fel tarteeb betkharab el bytes).
Python standard library bas, w byeshta3'al 3ala Linux w Windows.
"""
import hashlib
import socket
import struct
import time
from collections import namedtuple

# ===== el header =====
# kol packet beybda2 b header sabet 12 byte (el project beyseb7 lel 16 byte).
# el header beyoully el mosta2bel: meen el ba3at, eh no3 el resala, w kam byte gayeen ba3deh.
# HEADER_FMT: ! = network byte order, H = 2 byte, B = 1 byte, I = 4 byte.
MAGIC = 0xCA57
VERSION = 1
HEADER_FMT = "!HBBIHH"        # MAGIC(2) | version+type(1) | flags(1) | seq(4) | peer_id(2) | length(2)
HEADER_SIZE = struct.calcsize(HEADER_FMT)    # = 12 byte
MAX_DATAGRAM = 1200           # a2sa 7agm lel packet kolo (header + payload), el MTU limit bta3 el project
MAX_PAYLOAD = MAX_DATAGRAM - HEADER_SIZE     # = 1188 byte lel payload

# ===== anwa3 el rasayel w el flags =====
# el no3 beyetkateb fel 4 bits el so3'ayyara men byte raqm 2. kol flag bit wa7ed fel byte raqm 3.
INIT, PEER_LIST, HEARTBEAT, CHUNK_BITMAP, CHUNK_REQ, CHUNK_DATA, MAP_COMPLETE = range(7)
TYPE_NAMES = ["INIT", "PEER_LIST", "HEARTBEAT", "CHUNK_BITMAP",
              "CHUNK_REQ", "CHUNK_DATA", "MAP_COMPLETE"]
FLAG_RETX = 0x01              # el packet da e3adet ersal
FLAG_REPLY = 0x02             # el HEARTBEAT da rad 3ala ping

# ===== constants el project =====
SERVER_ID = 0
SERVER_PORT = 9000
NUM_CHUNKS = 256              # 4096 khana fel map / 16 khana fel chunk
CHUNK_SIZE = 48               # 16 khana x 3 byte
NUM_CLIENTS = 4
SLICE_SIZE = NUM_CHUNKS // NUM_CLIENTS       # kam chunk el server beyseedhom lekol client (64)
CHUNK_TTL = 0.5               # el wa2t (b el sanya) el nestanah ba3deh nes2al tani (500 ms)
SERVER_RATE_KBPS = 50         # el server mamnoo3 yeb3at aktar men 50 kbps

# Message = resala ba3d ma etfakket: kol 7aga laha esm (msg.type, msg.seq, ...)
Message = namedtuple("Message", "version type flags seq peer_id payload")


# byet7awwel raqm el no3 le esmo el maktoob 3ashan el log yeb2a mafhoom.
def type_name(t):
    return TYPE_NAMES[t] if 0 <= t < len(TYPE_NAMES) else "TYPE%d" % t


# ===== pack / unpack =====
# pack: men el arkam le bytes (header + payload). unpack: el 3aks, w beyrag3 None law el packet mesh sa7ee7.
def pack(msg_type, seq, peer_id, payload=b"", flags=0):
    if len(payload) > MAX_PAYLOAD:
        raise ValueError("payload too large")
    ver_type = (VERSION << 4) | (msg_type & 0x0F)
    header = struct.pack(HEADER_FMT, MAGIC, ver_type, flags,
                         seq & 0xFFFFFFFF, peer_id, len(payload))
    return header + payload


def unpack(data):
    if len(data) < HEADER_SIZE or len(data) > MAX_DATAGRAM:
        return None
    magic, ver_type, flags, seq, peer_id, length = struct.unpack(
        HEADER_FMT, data[:HEADER_SIZE])
    if magic != MAGIC:
        return None
    if (ver_type >> 4) != VERSION:
        return None
    if len(data) != HEADER_SIZE + length:
        return None
    return Message(ver_type >> 4, ver_type & 0x0F, flags, seq, peer_id,
                   data[HEADER_SIZE:])


# ===== el payload helpers =====
# el payload hwa "el gawab" gowwa el envelope, w kol no3 resala leh shakl.
# kol no3 leh dalet betgahhez el bytes (..._payload) w dalet betefta7o (parse_...).
def init_payload(listen_port):
    return struct.pack("!H", listen_port)


def parse_init(payload):
    return struct.unpack("!H", payload)[0]


def peer_list_payload(your_id, peers):
    out = struct.pack("!HB", your_id, len(peers))
    for pid, ip, port in peers:
        out += struct.pack("!H4sH", pid, socket.inet_aton(ip), port)
    return out


def parse_peer_list(payload):
    your_id, count = struct.unpack("!HB", payload[:3])
    peers = []
    for k in range(count):
        pid, ip, port = struct.unpack("!H4sH", payload[3 + 8 * k: 11 + 8 * k])
        peers.append((pid, socket.inet_ntoa(ip), port))
    return your_id, peers


def chunk_req_payload(chunk_id):
    return struct.pack("!H", chunk_id)


def chunk_data_payload(chunk_id, data):
    return struct.pack("!H", chunk_id) + data


def parse_chunk_id(payload):
    return struct.unpack("!H", payload[:2])[0]


# ===== el bitmap =====
# 256 bit (32 byte), kol bit lel chunk: 1 = ana 3andi el chunk da. (Phase 2: el heartbeat/bitmap bein el clients)
def bitmap_pack(have):
    bits = bytearray(NUM_CHUNKS // 8)
    for i in have:
        bits[i // 8] |= 1 << (i % 8)
    return bytes(bits)


def bitmap_unpack(raw):
    return {i for i in range(NUM_CHUNKS) if raw[i // 8] & (1 << (i % 8))}


# ===== el chunks el sabta =====
# make_chunk(i): el 48 byte bta3et el chunk raqm i (SHA-256), nafs el natee3a 3ala ay gehaz,
# fa ay client yeqdar yet2akked en el chunk el wasal sa7ee7. slice_of: el chunks el server beyseedha lekol client.
def make_chunk(i):
    out = b""
    k = 0
    while len(out) < CHUNK_SIZE:
        out += hashlib.sha256(("casp36-chunk:%d:%d" % (i, k)).encode()).digest()
        k += 1
    return out[:CHUNK_SIZE]


def slice_of(client_id):
    start = (client_id - 1) * SLICE_SIZE
    return range(start, start + SLICE_SIZE)


# ===== el PacketLog =====
# beyektob satr CSV lekol packet etb3atet aw wesselet ma3 el wa2t. da el log elly bet3rodo fel video w el report.
class PacketLog:
    HEADER = "time_ms,epoch_s,dir,type,seq,peer_id,flags,payload_len,bytes,remote,note\n"

    def __init__(self, path):
        self.t0 = time.monotonic()
        self.f = open(path, "w", buffering=1, newline="")
        self.f.write(self.HEADER)

    def log(self, direction, data, remote, note=""):
        msg = unpack(data)
        if msg is None:
            row = (direction, "BAD", "", "", "", "", len(data))
        else:
            row = (direction, type_name(msg.type), msg.seq, msg.peer_id,
                   msg.flags, len(msg.payload), len(data))
        self.f.write("%.3f,%.6f,%s,%s,%s,%s,%s,%s,%s,%s:%s,%s\n" % (
            (time.monotonic() - self.t0) * 1000.0, time.time(),
            row[0], row[1], row[2], row[3], row[4], row[5], row[6],
            remote[0], remote[1], note))

    def close(self):
        self.f.close()
