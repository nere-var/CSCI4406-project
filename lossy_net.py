#!/usr/bin/env python3
import argparse
import socket
import threading
import random
import time
import struct
from typing import Tuple, Dict, Optional

BUF_SIZE = 65535

# Must match chat_transport.py
HDR_FMT = "!BBHIIHHI"
HDR_SIZE = struct.calcsize(HDR_FMT)


def parse_args():
    p = argparse.ArgumentParser(description="UDP lossy shim (multi-client)")
    p.add_argument("--listen-host", default="127.0.0.1",
                   help="Host to listen on for clients (shim front)")
    p.add_argument("--listen-port", type=int, required=True,
                   help="Port clients connect to (shim front)")
    p.add_argument("--target-host", default="127.0.0.1",
                   help="Real server host")
    p.add_argument("--target-port", type=int, required=True,
                   help="Real server port")
    p.add_argument("--profile", default="random",
                   choices=["perfect", "random", "bursty"],
                   help="Loss profile")
    p.add_argument("--seed", type=int, default=None,
                   help="Random seed for reproducibility")
    return p.parse_args()


class LossModel:
    """
    Simple lossy behavior: drop / delay / duplicate / reorder.
    Tuned via 'profile', not meant to be exact.
    """

    def __init__(self, profile: str, seed: Optional[int] = None):
        if seed is not None:
            random.seed(seed)

        if profile == "perfect":
            self.drop_p = 0.0
            self.dup_p = 0.0
            self.delay_p = 0.0
            self.reorder_p = 0.0
        elif profile == "random":
            # Mild random loss
            self.drop_p = 0.05
            self.dup_p = 0.05
            self.delay_p = 0.05
            self.reorder_p = 0.05
        elif profile == "bursty":
            # A bit nastier
            self.drop_p = 0.1
            self.dup_p = 0.1
            self.delay_p = 0.15
            self.reorder_p = 0.15
        else:
            raise ValueError(f"Unknown profile: {profile}")

        self._reorder_buf = None  # type: Optional[Tuple[bytes, Tuple[str, int]]]
        self._lock = threading.Lock()

    def _do_send(self, sock: socket.socket, data: bytes, addr: Tuple[str, int]):
        try:
            sock.sendto(data, addr)
        except OSError:
            # Socket probably closed; ignore
            pass

    def _schedule_send(self, sock: socket.socket, data: bytes, addr: Tuple[str, int],
                       delay: float):
        t = threading.Timer(delay, self._do_send, args=(sock, data, addr))
        t.daemon = True
        t.start()

    def send_with_loss(self, sock: socket.socket, data: bytes, addr: Tuple[str, int]) -> None:
        """
        Apply loss/delay/dup/reorder before actually sending.
        """

        # Drop?
        if random.random() < self.drop_p:
            return

        # Reordering: keep one packet in buffer and swap order with next.
        to_send_first = None
        with self._lock:
            if self._reorder_buf is not None:
                # Send buffered one first, then process current as normal
                to_send_first = self._reorder_buf
                self._reorder_buf = None
            elif random.random() < self.reorder_p:
                # Buffer this packet and return; it will be sent when the next comes
                self._reorder_buf = (data, addr)
                return

        if to_send_first is not None:
            self._maybe_send_packet(sock, to_send_first[0], to_send_first[1])

        # Now handle the current packet normally (delay + dup)
        self._maybe_send_packet(sock, data, addr)

    def _maybe_send_packet(self, sock: socket.socket, data: bytes, addr: Tuple[str, int]) -> None:
        # Delay?
        if random.random() < self.delay_p:
            delay = random.uniform(0.02, 0.3)
            self._schedule_send(sock, data, addr, delay)
        else:
            self._do_send(sock, data, addr)

        # Duplicate?
        if random.random() < self.dup_p:
            delay = random.uniform(0.01, 0.1)
            self._schedule_send(sock, data, addr, delay)


def extract_conn_id(packet: bytes) -> Optional[int]:
    """
    Parse chat_transport header and return conn_id, or None if invalid.
    """
    if len(packet) < HDR_SIZE:
        return None
    try:
        ver, flags, conn_id, seq, ack, wnd, length, cksum = \
            struct.unpack(HDR_FMT, packet[:HDR_SIZE])
    except struct.error:
        return None
    return conn_id


def client_to_server_loop(sock_front: socket.socket,
                          sock_back: socket.socket,
                          loss_model: LossModel,
                          server_addr: Tuple[str, int],
                          conn_to_client: Dict[int, Tuple[str, int]],
                          lock: threading.Lock):
    """
    Forward packets from many clients to the single server.
    Build/maintain conn_id -> client_addr mapping.
    """
    while True:
        try:
            data, client_addr = sock_front.recvfrom(BUF_SIZE)
        except OSError:
            break

        conn_id = extract_conn_id(data)
        if conn_id is not None:
            with lock:
                conn_to_client[conn_id] = client_addr

        loss_model.send_with_loss(sock_back, data, server_addr)


def server_to_client_loop(sock_back: socket.socket,
                          sock_front: socket.socket,
                          loss_model: LossModel,
                          conn_to_client: Dict[int, Tuple[str, int]],
                          lock: threading.Lock):
    """
    Forward packets from server back to the appropriate client, using conn_id
    to decide which client address to send to.
    """
    while True:
        try:
            data, addr = sock_back.recvfrom(BUF_SIZE)
        except OSError:
            break

        conn_id = extract_conn_id(data)
        if conn_id is None:
            # Can't route; drop
            continue

        with lock:
            client_addr = conn_to_client.get(conn_id)

        if client_addr is None:
            # No known client for this conn_id yet; drop
            continue

        loss_model.send_with_loss(sock_front, data, client_addr)


def main():
    args = parse_args()

    if args.seed is not None:
        random.seed(args.seed)

    # Front socket: where clients send to (shim side)
    sock_front = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock_front.bind((args.listen_host, args.listen_port))

    # Back socket: shim ↔ server
    sock_back = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    # Bind to ephemeral port on same host so server can send back
    sock_back.bind((args.listen_host, 0))

    server_addr = (args.target_host, args.target_port)
    loss_model = LossModel(args.profile, seed=args.seed)

    # Multi-client mapping: conn_id -> client_addr
    conn_to_client: Dict[int, Tuple[str, int]] = {}
    lock = threading.Lock()

    print(f"Shim listening on {args.listen_host}:{args.listen_port}, "
          f"forwarding to {args.target_host}:{args.target_port} "
          f"(profile={args.profile})")

    t_c2s = threading.Thread(
        target=client_to_server_loop,
        args=(sock_front, sock_back, loss_model, server_addr, conn_to_client, lock),
        daemon=True,
    )
    t_s2c = threading.Thread(
        target=server_to_client_loop,
        args=(sock_back, sock_front, loss_model, conn_to_client, lock),
        daemon=True,
    )

    t_c2s.start()
    t_s2c.start()

    try:
        while True:
            time.sleep(1.0)
    except KeyboardInterrupt:
        print("Shim exiting...")
        try:
            sock_front.close()
        except OSError:
            pass
        try:
            sock_back.close()
        except OSError:
            pass


if __name__ == "__main__":
    main()
