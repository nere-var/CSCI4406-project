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
    """
    Sliding-window (Go-Back-N) reliable transport over UDP.

    - Message-oriented API: connect(), send_msg(), on_message(), close()
    - Sender keeps a window of unACKed packets (size = WINDOW_SIZE).
    - Receiver enforces in-order delivery; out-of-order packets are dropped
      and recovered via retransmission from the sender.
    """

    WINDOW_SIZE = 10
    TIMER_GRANULARITY = 0.01  # seconds

    def __init__(self, local_addr=('0.0.0.0', 0), conn_id=None):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(local_addr)

        self.peer = None          # default send target
        self.last_from = None     # last address we heard from
        self.conn_id = conn_id if conn_id is not None else (
                int(time.time() * 1000) & 0xFFFF
        )

        # --- sender-side state (Go-Back-N) ---
        self.send_base = 1                # seq number of oldest unACKed packet
        self.next_seq = 1                 # next seq to use
        self.unacked = {}                 # seq -> (pkt_bytes, send_ts)
        self.rto = DEFAULT_RTO            # retransmission timeout (seconds)

        # --- receiver-side state ---
        self.expect_seq = 1               # next in-order seq we expect

        self._closed = False
        self._on_msg = None
        self._lock = threading.RLock()

        self.metrics = {
            'bytes_sent': 0,
            'bytes_recv': 0,
            'data_msgs_sent': 0,
            'data_msgs_recv': 0,
            'retransmissions': 0,
            'out_of_order': 0,
            # (send_ts, ack_recv_ts) pairs for latency stats
            'latency_samples': []
        }

        # background threads for RX and timers
        threading.Thread(target=self._rx_loop, daemon=True).start()
        threading.Thread(target=self._timer_loop, daemon=True).start()

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
        """
        Reliably send one message (<= MAX_PAYLOAD) using a sliding window.

        Blocks briefly if the window is full (len(unacked) >= WINDOW_SIZE).
        """
        assert len(data) <= MAX_PAYLOAD, "payload too large"

        with self._lock:
            # wait for space in the window based on unACKed count
            while len(self.unacked) >= self.WINDOW_SIZE and not self._closed:
                time.sleep(0.002)

            if self._closed:
                return

            seq = self.next_seq
            self.next_seq += 1

            pkt = self._make_pkt(FLAG_DATA, seq, 0, data)
            send_ts = time.time()
            self.unacked[seq] = (pkt, send_ts)

            self.sock.sendto(pkt, self.peer)
            self.metrics['bytes_sent'] += len(pkt)
            self.metrics['data_msgs_sent'] += 1

            # if this is the first packet in-flight, make sure send_base tracks it
            if len(self.unacked) == 1:
                self.send_base = seq

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

    # ---------------- Internal: RX + Timers ----------------
    def _rx_loop(self):
        """Receive packets, handle ACK/DATA/FIN."""
        while not self._closed:
            try:
                data, addr = self.sock.recvfrom(2048)
            except OSError:
                break
            recv_ts = time.time()
            self.metrics['bytes_recv'] += len(data)

            self.last_from = addr
            if self.peer is None:
                self.peer = addr

            if len(data) < HDR_SIZE:
                continue

            hdr = data[:HDR_SIZE]
            payload = data[HDR_SIZE:]
            if not self._valid_cksum(hdr, payload):
                continue

            ver, flags, conn_id, seq, ack, wnd, length, cksum = struct.unpack(
                HDR_FMT, hdr
            )

            # --- ACK handling (cumulative Go-Back-N) ---
            if flags & FLAG_ACK:
                with self._lock:
                    if ack > 0 and self.unacked:
                        # ack is "next expected" from receiver; everything < ack is delivered
                        to_delete = [s for s in self.unacked if s < ack]
                        for s in sorted(to_delete):
                            send_ts = self.unacked[s][1]
                            self.metrics['latency_samples'].append((send_ts, recv_ts))
                            del self.unacked[s]

                        if self.unacked:
                            self.send_base = min(self.unacked.keys())
                        else:
                            # nothing in-flight, move base up
                            self.send_base = ack

            # --- DATA handling ---
            if flags & FLAG_DATA:
                with self._lock:
                    if seq == self.expect_seq:
                        # in order → deliver and advance
                        self._deliver(payload)
                        self.expect_seq += 1
                    elif seq > self.expect_seq:
                        # out-of-order arrival; counted but not delivered
                        self.metrics['out_of_order'] += 1

                    # send cumulative ACK for next expected seq
                    self._send_ack(self.expect_seq)
                    self.metrics['data_msgs_recv'] += 1

            # --- FIN handling ---
            if flags & FLAG_FIN:
                with self._lock:
                    self._send_ack(self.expect_seq)
                    self._closed = True

    def _timer_loop(self):
        """
        Periodically check for timeouts.

        If the oldest unACKed packet has been outstanding longer than RTO,
        retransmit ALL unACKed packets (Go-Back-N behavior).
        """
        while not self._closed:
            time.sleep(self.TIMER_GRANULARITY)
            with self._lock:
                if not self.unacked or self._closed:
                    continue

                oldest_seq = min(self.unacked.keys())
                pkt, send_ts = self.unacked[oldest_seq]
                if time.time() - send_ts >= self.rto:
                    # timeout: retransmit all packets currently unACKed
                    for s in sorted(self.unacked.keys()):
                        pkt_s, _ = self.unacked[s]
                        self.sock.sendto(pkt_s, self.peer)
                        self.unacked[s] = (pkt_s, time.time())
                        self.metrics['bytes_sent'] += len(pkt_s)
                        self.metrics['retransmissions'] += 1

    # ---------------- Helpers ----------------
    def _deliver(self, payload: bytes):
        """Deliver received payload to app callback."""
        if self._on_msg:
            try:
                self._on_msg(payload)
            except Exception:
                pass

    def _send_ack(self, ackno):
        """Send ACK for the *next* expected sequence number."""
        ack_hdr = self._pack_hdr(FLAG_ACK, 0, ackno, 0, 0)
        self.sock.sendto(ack_hdr, self.last_from or self.peer)
        self.metrics['bytes_sent'] += len(ack_hdr)

    def _make_pkt(self, flags, seq, ack, payload: bytes):
        """Build header+payload with CRC32 checksum."""
        hdr0 = struct.pack(
            HDR_FMT, VER, flags, self.conn_id, seq, ack, 0, len(payload), 0
        )
        cksum = zlib.crc32(hdr0 + payload) & 0xFFFFFFFF
        hdr = struct.pack(
            HDR_FMT, VER, flags, self.conn_id, seq, ack, 0, len(payload), cksum
        )
        return hdr + payload

    def _pack_hdr(self, flags, seq, ack, wnd, length):
        """Build header-only packet (SYN/ACK/FIN)."""
        hdr0 = struct.pack(
            HDR_FMT, VER, flags, self.conn_id, seq, ack, wnd, length, 0
        )
        cksum = zlib.crc32(hdr0) & 0xFFFFFFFF
        return struct.pack(
            HDR_FMT, VER, flags, self.conn_id, seq, ack, wnd, length, cksum
        )

    def _valid_cksum(self, hdr, payload=b''):
        """Verify CRC32 checksum."""
        ver, flags, conn_id, seq, ack, wnd, length, cksum = struct.unpack(
            HDR_FMT, hdr
        )
        hdr0 = struct.pack(
            HDR_FMT, ver, flags, conn_id, seq, ack, wnd, length, 0
        )
        calc = zlib.crc32(hdr0 + payload) & 0xFFFFFFFF
        return calc == cksum
