#chat_client.py — command-line client for the group chat service

import argparse
import threading
import json

from chat_transport import ReliableUDPSocket


def to_bytes(d: dict) -> bytes:
    return (json.dumps(d) + "\n").encode("utf-8")


def from_bytes(b: bytes) -> dict:
    return json.loads(b.decode("utf-8").strip())


class ChatClient:
    def __init__(self, server_host: str, server_port: int):
        self.transport = ReliableUDPSocket()
        self.conn_id = self.transport.connect((server_host, server_port))
        self.running = True

    # ---------- auth ----------

    def _auth(self):
        while True:
            choice = input("Register (r) or Login (l): ").strip().lower()
            if choice not in ("r", "l"):
                continue
            username = input("Username: ").strip()
            password = input("Password: ").strip()

            msg = {
                "type": "auth",
                "mode": "register" if choice == "r" else "login",
                "username": username,
                "password": password,
            }
            self.transport.send_msg(self.conn_id, to_bytes(msg))
            reply_bytes = self.transport.recv_msg(self.conn_id, timeout=30.0)
            if not reply_bytes:
                print("No response from server, try again.")
                continue
            try:
                reply = from_bytes(reply_bytes)
            except Exception:
                print("Bad response from server")
                continue
            if reply.get("type") == "auth_ok":
                print(reply.get("message", "Authenticated."))
                return username
            else:
                print("Auth error:", reply.get("message"))

    # ---------- receiving loop ----------

    def _recv_loop(self):
        while self.running:
            data = self.transport.recv_msg(self.conn_id, timeout=1.0)
            if data is None:
                continue
            try:
                msg = from_bytes(data)
            except Exception:
                print("<< malformed message >>")
                continue
            mtype = msg.get("type")

            if mtype == "chat":
                room = msg.get("room")
                user = msg.get("user")
                text = msg.get("text")
                print(f"[{room}] {user}: {text}")
            elif mtype == "presence":
                event = msg.get("event")
                room = msg.get("room")
                user = msg.get("user")
                if event == "join":
                    print(f"* {user} joined {room}")
                elif event == "leave":
                    print(f"* {user} left {room}")
            elif mtype == "system":
                print(f"* {msg.get('message')}")
            elif mtype == "auth_error":
                print("Auth error from server:", msg.get("message"))
            # ignore any others

    # ---------- user input ----------

    def _input_loop(self, username: str):
        print("Commands:")
        print("  /join ROOM           join or create a room")
        print("  /leave ROOM          leave a room")
        print("  /msg ROOM text...    send message to room")
        print("  /quit                disconnect")
        print()
        while self.running:
            try:
                line = input("> ")
            except EOFError:
                break
            if not line:
                continue
            if line.startswith("/join "):
                room = line.split(maxsplit=1)[1].strip()
                self.transport.send_msg(
                    self.conn_id, to_bytes({"type": "join", "room": room})
                )
            elif line.startswith("/leave "):
                room = line.split(maxsplit=1)[1].strip()
                self.transport.send_msg(
                    self.conn_id, to_bytes({"type": "leave", "room": room})
                )
            elif line.startswith("/msg "):
                parts = line.split(maxsplit=2)
                if len(parts) < 3:
                    print("Usage: /msg ROOM text")
                    continue
                room = parts[1]
                text = parts[2]
                self.transport.send_msg(
                    self.conn_id,
                    to_bytes({"type": "msg", "room": room, "text": text}),
                )
            elif line.startswith("/quit"):
                self.transport.send_msg(
                    self.conn_id, to_bytes({"type": "logout"})
                )
                self.running = False
                break
            else:
                print("Unknown command. Use /join, /leave, /msg, /quit")

    # ---------- main ----------

    def run(self):
        username = self._auth()
        recv_t = threading.Thread(target=self._recv_loop, daemon=True)
        recv_t.start()
        try:
            self._input_loop(username)
        finally:
            self.running = False
            self.transport.close_conn(self.conn_id)
            self.transport.print_metrics("client")
            self.transport.shutdown()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--server", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9000)
    args = parser.parse_args()

    client = ChatClient(args.server, args.port)
    client.run()


if __name__ == "__main__":
    main()

