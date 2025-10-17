# chat_transport.py — Reliable-ish transport over UDP 


import socket
import struct
import threading
import time
import zlib

# Header format: ver(1B)|flags(1B)|conn_id(2B)|seq(4B)|ack(4B)|wnd(2B)|len(2B)|cksum(4B)
HDR_FMT = '!BBHIIHHI'
HDR_SIZE = struct.calcsize(HDR_FMT)
VER = 1

FLAG_SYN = 0x01
FLAG_ACK = 0x02
FLAG_FIN = 0x04
FLAG_DATA = 0x08

DEFAULT_RTO = 0.35
MAX_PAYLOAD = 900


class ReliableUDPSocket:
    """Stop-and-Wait reliable transport over UDP """
    def __init__(self, local_addr=('0.0.0.0', 0), conn_id=None):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(local_addr)
        self.peer = None                 # used for sending
        self.last_from = None           
        self.conn_id = conn_id if conn_id is not None else (int(time.time() * 1000) & 0xFFFF)

        self.next_seq = 1
        self.expect_seq = 1
        self.inflight = None
        self._closed = False
        self._on_msg = None
        self._lock = threading.RLock()

        self.metrics = {
            'bytes_sent': 0, 'bytes_recv': 0,
            'data_msgs_sent': 0, 'data_msgs_recv': 0,
            'retransmissions': 0, 'out_of_order': 0,
            'latency_samples': []
        }

        threading.Thread(target=self._rx_loop, daemon=True).start()

    # ---------------- Public API ----------------
    def connect(self, peer_addr):
        """Set remote peer and send SYN (no full handshake)."""
        self.peer = peer_addr
        syn = self._pack_hdr(FLAG_SYN | FLAG_ACK, 0, 0, 0, 0)
        self.sock.sendto(syn, self.peer)
        self.metrics['bytes_sent'] += len(syn)

    def on_message(self, cb):
        """Register callback to deliver messages (payload bytes)."""
        self._on_msg = cb

    def send_msg(self, data: bytes):
        """Reliably send one message (<= MAX_PAYLOAD)."""
        assert len(data) <= MAX_PAYLOAD, "payload too large"
        with self._lock:
            while self.inflight is not None and not self._closed:
                time.sleep(0.002)
            seq = self.next_seq
            self.next_seq += 1
            pkt = self._make_pkt(FLAG_DATA, seq, self.expect_seq, data)
            self.inflight = (seq, pkt, time.time())
            self.sock.sendto(pkt, self.peer)
            self.metrics['bytes_sent'] += len(pkt)
            self.metrics['data_msgs_sent'] += 1
            threading.Thread(target=self._timer, args=(seq,), daemon=True).start()

    def close(self):
        """Send FIN and close socket."""
        with self._lock:
            self._closed = True
            try:
                fin = self._make_pkt(FLAG_FIN, self.next_seq, self.expect_seq, b'')
                self.sock.sendto(fin, self.peer)
                self.metrics['bytes_sent'] += len(fin)
            except Exception:
                pass
        time.sleep(0.05)
        try:
            self.sock.close()
        except Exception:
            pass

 
    def _rx_loop(self):
        """Receive packets, handle ACK/DATA/FIN."""
        while not self._closed:
            try:
                data, addr = self.sock.recvfrom(2048)
            except OSError:
                break
            recv_ts = time.time()
            self.metrics['bytes_recv'] += len(data)

            # Track the true sender of this packet
            self.last_from = addr

            # For convenience, if we don't have a default send target yet, adopt this one
            if self.peer is None:
                self.peer = addr

            hdr = data[:HDR_SIZE]
            payload = data[HDR_SIZE:]
            if not self._valid_cksum(hdr, payload):
                continue
            ver, flags, conn_id, seq, ack, wnd, length, cksum = struct.unpack(HDR_FMT, hdr)

            if flags & FLAG_ACK:
                with self._lock:
                    if self.inflight and ack >= self.inflight[0]:
                        send_ts = self.inflight[2]
                        self.metrics['latency_samples'].append((send_ts, recv_ts))
                        self.inflight = None

            if flags & FLAG_DATA:
                if seq == self.expect_seq:
                    self._deliver(payload)
                    self.expect_seq += 1
                else:
                    # count OOO but don't buffer
                    self.metrics['out_of_order'] += 1
                self._send_ack(self.expect_seq)
                self.metrics['data_msgs_recv'] += 1

            if flags & FLAG_FIN:
                self._send_ack(self.expect_seq)
                self._closed = True

    def _timer(self, seq):
        """Retransmit once if ACK not received before timeout."""
        time.sleep(DEFAULT_RTO)
        with self._lock:
            if self.inflight and self.inflight[0] == seq and not self._closed:
                self.sock.sendto(self.inflight[1], self.peer)
                self.metrics['bytes_sent'] += len(self.inflight[1])
                self.metrics['retransmissions'] += 1

    def _deliver(self, payload: bytes):
        """Deliver received payload to app callback."""
        if self._on_msg:
            try:
                self._on_msg(payload)
            except Exception:
                pass

    def _send_ack(self, ackno):
        """Send ACK for next expected seq."""
        ack_hdr = self._pack_hdr(FLAG_ACK, 0, ackno, 0, 0)
        self.sock.sendto(ack_hdr, self.last_from or self.peer)
        self.metrics['bytes_sent'] += len(ack_hdr)

    def _make_pkt(self, flags, seq, ack, payload: bytes):
        """Build header+payload with CRC32 checksum."""
        hdr0 = struct.pack(HDR_FMT, VER, flags, self.conn_id, seq, ack, 0, len(payload), 0)
        cksum = zlib.crc32(hdr0 + payload) & 0xFFFFFFFF
        hdr = struct.pack(HDR_FMT, VER, flags, self.conn_id, seq, ack, 0, len(payload), cksum)
        return hdr + payload

    def _pack_hdr(self, flags, seq, ack, wnd, length):
        """Build header-only packet (SYN/ACK/FIN)."""
        hdr0 = struct.pack(HDR_FMT, VER, flags, self.conn_id, seq, ack, wnd, length, 0)
        cksum = zlib.crc32(hdr0) & 0xFFFFFFFF
        return struct.pack(HDR_FMT, VER, flags, self.conn_id, seq, ack, wnd, length, cksum)

    def _valid_cksum(self, hdr, payload=b''):
        """Verify CRC32 checksum."""
        ver, flags, conn_id, seq, ack, wnd, length, cksum = struct.unpack(HDR_FMT, hdr)
        hdr0 = struct.pack(HDR_FMT, ver, flags, conn_id, seq, ack, wnd, length, 0)
        calc = zlib.crc32(hdr0 + payload) & 0xFFFFFFFF
        return calc == cksum

