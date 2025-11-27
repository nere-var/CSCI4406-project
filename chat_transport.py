import socket
import struct
import threading
import time
import zlib
import random
import queue
from typing import Dict, Tuple, Optional, Any, List

# Header format: ver(1B)|flags(1B)|conn_id(2B)|seq(4B)|ack(4B)|wnd(2B)|len(2B)|cksum(4B)
HDR_FMT = "!BBHIIHHI"
HDR_SIZE = struct.calcsize(HDR_FMT)
VER = 1

FLAG_SYN = 0x01
FLAG_ACK = 0x02
FLAG_FIN = 0x04
FLAG_DATA = 0x08

MAX_PAYLOAD = 1000        # bytes of app data per packet
WINDOW_SIZE = 16          # sliding window size (Go-Back-N)
RTO = 0.3                 # retransmission timeout (seconds)


class ConnectionState:
    """Per-connection sliding-window state."""

    def __init__(self, conn_id: int, addr: Tuple[str, int]):
        self.conn_id = conn_id
        self.addr = addr  # remote address

        # sending side
        self.send_base = 0
        self.next_seq = 0
        self.window = WINDOW_SIZE
        # seq -> (packet_bytes, last_send_time)
        self.unacked: Dict[int, Tuple[bytes, float]] = {}

        # receiving side
        self.recv_base = 0
        self.recv_buffer: Dict[int, bytes] = {}

        # app-facing incoming queue (None means connection closed)
        self.incoming: "queue.Queue[Optional[bytes]]" = queue.Queue()

        # handshake / close
        self.established = False
        self.closed = False
        self.fin_sent = False
        self.fin_acked = False

        self.handshake_ev = threading.Event()

        # metrics
        self.send_times: Dict[int, float] = {}   # seq -> time first sent
        self.latencies: List[float] = []

        self.out_of_order = 0
        self.retransmissions = 0
        self.bytes_sent = 0
        self.bytes_payload = 0

        # lock to guard sending state
        self.lock = threading.Lock()


class ReliableUDPSocket:
    """
    Simple message-oriented reliable transport over UDP using Go-Back-N.

    API (message-oriented):

        # Client
        sock = ReliableUDPSocket()
        conn_id = sock.connect((server_host, server_port))
        sock.send_msg(conn_id, b"hello")
        data = sock.recv_msg(conn_id, timeout=5.0)

        # Server
        sock = ReliableUDPSocket(listen_addr=("0.0.0.0", 9000), is_server=True)
        conn_id, addr = sock.accept()
        ...
    """

    def __init__(self, listen_addr: Optional[Tuple[str, int]] = None, is_server: bool = False):
        self.is_server = is_server
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        if listen_addr is not None:
            self.sock.bind(listen_addr)

        # conn_id -> ConnectionState
        self.conns: Dict[int, ConnectionState] = {}
        self.conns_lock = threading.Lock()

        # queue for newly accepted connections (server side)
        self.accept_queue: "queue.Queue[Tuple[int, Tuple[str, int]]]" = queue.Queue()

        self.running = True
        self.recv_thread = threading.Thread(target=self._recv_loop, daemon=True)
        self.timer_thread = threading.Thread(target=self._timer_loop, daemon=True)
        self.recv_thread.start()
        self.timer_thread.start()

    # -------- public API --------

    def connect(self, addr: Tuple[str, int], timeout: float = 5.0) -> int:
        """
        Client side: create a connection object for (addr) and return conn_id.

        For our project we don't need a full TCP-style 3-way handshake.
        We just assign a conn_id, remember the peer, send one SYN so the
        server sees us, and immediately treat the connection as established.
        """
        with self.conns_lock:
            conn_id = random.randint(1, 0xFFFF)
            while conn_id in self.conns:
                conn_id = random.randint(1, 0xFFFF)
            state = ConnectionState(conn_id, addr)
            self.conns[conn_id] = state

        # optional "hello" so the server's recv_loop can enqueue us
        syn_hdr = self._pack_header(
            flags=FLAG_SYN,
            conn_id=conn_id,
            seq=0,
            ack=0,
            wnd=WINDOW_SIZE,
            payload=b"",
        )
        self.sock.sendto(syn_hdr, addr)
        state.bytes_sent += len(syn_hdr)

        # ✅ do NOT wait on handshake_ev / do NOT raise TimeoutError
        state.established = True
        state.handshake_ev.set()   # mark as ready just in case anyone checks it

        return conn_id


    def accept(self, timeout: Optional[float] = None) -> Tuple[int, Tuple[str, int]]:
        """
        Server side: wait for a new connection.
        Returns (conn_id, addr).
        """
        try:
            return self.accept_queue.get(timeout=timeout)
        except queue.Empty:
            raise TimeoutError("No incoming connections")

    def send_msg(self, conn_id: int, payload: bytes) -> None:
        """Send one application message reliably."""
        if len(payload) > MAX_PAYLOAD:
            raise ValueError(
                f"Payload too large (>{MAX_PAYLOAD} bytes); fragmentation not implemented"
            )

        state = self.conns.get(conn_id)
        if state is None or state.closed:
            raise RuntimeError("Connection not available")

        with state.lock:
            # wait if window is full
            while state.next_seq >= state.send_base + state.window:
                time.sleep(0.01)

            seq = state.next_seq
            state.next_seq += 1
            pkt = self._pack_header(
                flags=FLAG_DATA,
                conn_id=conn_id,
                seq=seq,
                ack=0,
                wnd=state.window,
                payload=payload,
            )
            now = time.time()
            self.sock.sendto(pkt, state.addr)
            state.unacked[seq] = (pkt, now)
            state.send_times.setdefault(seq, now)
            state.bytes_sent += len(pkt)
            state.bytes_payload += len(payload)

    def recv_msg(self, conn_id: int, timeout: Optional[float] = None) -> Optional[bytes]:
        """Receive next in-order application message for this connection."""
        state = self.conns.get(conn_id)
        if state is None:
            return None
        try:
            data = state.incoming.get(timeout=timeout)
            return data
        except queue.Empty:
            return None

    def close_conn(self, conn_id: int) -> None:
        """Initiate a graceful close for a single connection."""
        state = self.conns.get(conn_id)
        if state is None or state.closed:
            return
        with state.lock:
            if state.fin_sent:
                return
            pkt = self._pack_header(
                flags=FLAG_FIN,
                conn_id=conn_id,
                seq=state.next_seq,
                ack=0,
                wnd=state.window,
                payload=b"",
            )
            self.sock.sendto(pkt, state.addr)
            state.bytes_sent += len(pkt)
            state.fin_sent = True

    def shutdown(self) -> None:
        """Stop all activity and close underlying socket."""
        self.running = False
        try:
            self.sock.close()
        except Exception:
            pass

    # -------- internal helpers --------

    def _pack_header(
            self,
            flags: int,
            conn_id: int,
            seq: int,
            ack: int,
            wnd: int,
            payload: bytes,
    ) -> bytes:
        length = len(payload)
        hdr_wo_cksum = struct.pack(
            HDR_FMT, VER, flags, conn_id, seq, ack, wnd, length, 0
        )
        cksum = zlib.crc32(hdr_wo_cksum + payload) & 0xFFFFFFFF
        return struct.pack(
            HDR_FMT, VER, flags, conn_id, seq, ack, wnd, length, cksum
        ) + payload

    def _unpack(self, packet: bytes):
        hdr = packet[:HDR_SIZE]
        payload = packet[HDR_SIZE:]
        ver, flags, conn_id, seq, ack, wnd, length, cksum = struct.unpack(
            HDR_FMT, hdr
        )
        if ver != VER:
            raise ValueError("Bad version")
        if length != len(payload):
            raise ValueError("Length mismatch")

        hdr0 = struct.pack(HDR_FMT, ver, flags, conn_id, seq, ack, wnd, length, 0)
        calc = zlib.crc32(hdr0 + payload) & 0xFFFFFFFF
        if calc != cksum:
            raise ValueError("Bad checksum")
        return ver, flags, conn_id, seq, ack, wnd, length, cksum, payload

    # -------- background loops --------

    def _recv_loop(self) -> None:
        while self.running:
            try:
                packet, addr = self.sock.recvfrom(4096)
            except OSError:
                break
            try:
                ver, flags, conn_id, seq, ack, wnd, length, cksum, payload = \
                    self._unpack(packet)
            except Exception:
                # drop bad packets silently
                continue

            # get or create state
            with self.conns_lock:
                state = self.conns.get(conn_id)
                if state is None:
                    # only accept new if this endpoint is server and packet is SYN
                    if self.is_server and (flags & FLAG_SYN):
                        state = ConnectionState(conn_id, addr)
                        self.conns[conn_id] = state
                    else:
                        continue

            if flags & FLAG_SYN:
                # respond with SYN-ACK
                syn_ack = self._pack_header(
                    FLAG_SYN | FLAG_ACK,
                    conn_id,
                    seq=0,
                    ack=seq + 1,
                    wnd=WINDOW_SIZE,
                    payload=b"",
                    )
                self.sock.sendto(syn_ack, addr)
                state.bytes_sent += len(syn_ack)
                state.established = True
                state.handshake_ev.set()
                if self.is_server:
                    self.accept_queue.put((conn_id, addr))
                continue

            if flags & FLAG_ACK:
                # used for SYN-ACK on client side and for DATA acks
                if not state.established:
                    state.established = True
                    state.handshake_ev.set()

                # cumulative ACK for data (Go-Back-N)
                with state.lock:
                    if ack > state.send_base:
                        to_remove = [s for s in state.unacked.keys() if s < ack]
                        now = time.time()
                        for s in to_remove:
                            pkt, t0 = state.unacked.pop(s)
                            if s in state.send_times:
                                state.latencies.append(now - state.send_times[s])
                                del state.send_times[s]
                        state.send_base = ack

            if flags & FLAG_DATA:
                # receiving side reliability
                if seq < state.recv_base:
                    # already have it; re-ACK
                    self._send_ack(state, addr)
                    continue

                if seq > state.recv_base:
                    # out-of-order; buffer
                    if seq not in state.recv_buffer:
                        state.recv_buffer[seq] = payload
                        state.out_of_order += 1
                    self._send_ack(state, addr)
                    continue

                # seq == recv_base: in-order
                state.incoming.put(payload)
                state.recv_base += 1
                # deliver any buffered successors
                while state.recv_base in state.recv_buffer:
                    state.incoming.put(state.recv_buffer.pop(state.recv_base))
                    state.recv_base += 1

                self._send_ack(state, addr)

            if flags & FLAG_FIN:
                # remote wants to close
                state.closed = True
                state.incoming.put(None)
                fin_ack = self._pack_header(
                    FLAG_ACK,
                    conn_id,
                    seq=0,
                    ack=seq + 1,
                    wnd=state.window,
                    payload=b"",
                )
                self.sock.sendto(fin_ack, addr)
                state.bytes_sent += len(fin_ack)

    def _send_ack(self, state: ConnectionState, addr: Tuple[str, int]) -> None:
        ackno = state.recv_base
        ack_pkt = self._pack_header(
            FLAG_ACK,
            state.conn_id,
            seq=0,
            ack=ackno,
            wnd=state.window,
            payload=b"",
        )
        self.sock.sendto(ack_pkt, addr)
        state.bytes_sent += len(ack_pkt)

    def _timer_loop(self) -> None:
        """Periodically scan all connections and retransmit oldest unacked packet if needed."""
        while self.running:
            time.sleep(0.05)
            now = time.time()
            with self.conns_lock:
                states = list(self.conns.values())
            for state in states:
                with state.lock:
                    if not state.unacked:
                        continue
                    pkt_info = state.unacked.get(state.send_base)
                    if not pkt_info:
                        continue
                    pkt, last_time = pkt_info
                    if now - last_time >= RTO:
                        # Go-Back-N: retransmit all unacked from send_base upwards
                        for s in sorted(state.unacked.keys()):
                            pkt_s, _ = state.unacked[s]
                            self.sock.sendto(pkt_s, state.addr)
                            state.unacked[s] = (pkt_s, now)
                            state.retransmissions += 1

    # -------- metrics helpers --------

    def print_metrics(self, label: str = "transport") -> None:
        """Print basic metrics for debugging / reporting."""
        with self.conns_lock:
            conns = list(self.conns.values())
        print(f"=== Metrics for {label} ===")
        for st in conns:
            total_msgs = st.recv_base
            avg_lat = sum(st.latencies) / len(st.latencies) if st.latencies else 0.0
            p95 = 0.0
            if st.latencies:
                sorted_lats = sorted(st.latencies)
                idx = int(0.95 * (len(sorted_lats) - 1))
                p95 = sorted_lats[idx]
            kb_sent = st.bytes_payload / 1024.0 if st.bytes_payload else 0.0
            retrans_per_kb = (st.retransmissions / kb_sent) if kb_sent > 0 else 0.0
            print(f"Connection {st.conn_id} -> {st.addr}")
            print(f"  msgs delivered: {total_msgs}")
            print(f"  avg latency: {avg_lat*1000:.2f} ms  p95: {p95*1000:.2f} ms")
            print(f"  retransmissions: {st.retransmissions}  ({retrans_per_kb:.2f} per KB payload)")
            print(f"  out-of-order msgs: {st.out_of_order}")
