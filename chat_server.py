# chat_server.py — server with rooms + presence over ReliableUDPSocket
#made some changes, please run to see whats up 
import argparse
import threading
from collections import defaultdict
import json
import os
from typing import Dict, Set

from chat_transport import ReliableUDPSocket

ACCOUNTS_FILE = "accounts.json"  # username -> password mapping


def to_bytes(d: dict) -> bytes:
    """Encode a JSON object as bytes."""
    return (json.dumps(d) + "\n").encode("utf-8")


def from_bytes(b: bytes) -> dict:
    """Decode bytes into JSON object."""
    return json.loads(b.decode("utf-8").strip())


class ChatServer:
    def __init__(self, host: str, port: int):
        self.transport = ReliableUDPSocket(listen_addr=(host, port), is_server=True)

        # room_name -> set of conn_ids
        self.rooms: Dict[str, Set[int]] = defaultdict(set)
        self.conn_user: Dict[int, str] = {}
        self.conn_rooms: Dict[int, Set[str]] = defaultdict(set)

        # NEW: track which connections we've already started a thread for
        self.seen_conns = set()

        self.accounts_lock = threading.Lock()
        self._load_accounts()

    # ---------- account management ----------

    def _load_accounts(self):
        self.accounts = {}
        if os.path.exists(ACCOUNTS_FILE):
            try:
                with open(ACCOUNTS_FILE, "r", encoding="utf-8") as f:
                    self.accounts = json.load(f)
            except Exception:
                self.accounts = {}

    def _save_accounts(self):
        with self.accounts_lock:
            with open(ACCOUNTS_FILE, "w", encoding="utf-8") as f:
                json.dump(self.accounts, f, indent=2)

    # ---------- main loop ----------

    def serve_forever(self):
        print("Chat server running...")
        try:
            while True:
                try:
                    conn_id, addr = self.transport.accept()
                except TimeoutError:
                    continue

                # ✅ Ignore duplicate notifications for the same connection
                if conn_id in self.seen_conns:
                    continue
                self.seen_conns.add(conn_id)

                print(f"New connection {conn_id} from {addr}")
                t = threading.Thread(
                    target=self._handle_client, args=(conn_id,), daemon=True
                )
                t.start()
        except KeyboardInterrupt:
            print("Shutting down server...")
        finally:
            self.transport.print_metrics("server")
            self.transport.shutdown()

    # ---------- helpers ----------

    def _send(self, conn_id: int, obj: dict):
        self.transport.send_msg(conn_id, to_bytes(obj))

    def _broadcast_room(self, room: str, obj: dict):
        conns = list(self.rooms.get(room, []))
        for cid in conns:
            try:
                self._send(cid, obj)
            except Exception:
                pass


    def _handle_auth(self, conn_id: int) -> bool:
        """Handle login/register. Returns True if authenticated."""
        while True:
            msg_bytes = self.transport.recv_msg(conn_id, timeout=300)
            if msg_bytes is None:
                return False
            try:
                msg = from_bytes(msg_bytes)
            except Exception:
                self._send(
                    conn_id, {"type": "auth_error", "message": "Bad auth message"}
                )
                continue

            if msg.get("type") != "auth":
                self._send(
                    conn_id,
                    {
                        "type": "auth_error",
                        "message": "Please authenticate first",
                    },
                )
                continue

            mode = msg.get("mode")
            username = msg.get("username", "")
            password = msg.get("password", "")

            if not username or not password:
                self._send(
                    conn_id,
                    {
                        "type": "auth_error",
                        "message": "Username and password required",
                    },
                )
                continue

            with self.accounts_lock:
                if mode == "register":
                    if username in self.accounts:
                        self._send(
                            conn_id,
                            {
                                "type": "auth_error",
                                "message": "Username already exists",
                            },
                        )
                        continue
                    self.accounts[username] = password
                    self._save_accounts()
                elif mode == "login":
                    if self.accounts.get(username) != password:
                        self._send(
                            conn_id,
                            {
                                "type": "auth_error",
                                "message": "Invalid username or password",
                            },
                        )
                        continue
                else:
                    self._send(
                        conn_id,
                        {"type": "auth_error", "message": "Unknown auth mode"},
                    )
                    continue

            self.conn_user[conn_id] = username
            self._send(
                conn_id,
                {"type": "auth_ok", "message": f"Welcome {username}!"},
            )
            print(f"Connection {conn_id} authenticated as {username}")
            return True

    def _handle_client(self, conn_id: int):
        user = None
        try:
            if not self._handle_auth(conn_id):
                return
            user = self.conn_user.get(conn_id)

            while True:
                msg_bytes = self.transport.recv_msg(conn_id, timeout=None)
                if msg_bytes is None:
                    break
                try:
                    msg = from_bytes(msg_bytes)
                except Exception:
                    self._send(
                        conn_id, {"type": "system", "message": "Malformed message"}
                    )
                    continue

                mtype = msg.get("type")

                if mtype == "join":
                    room = msg.get("room", "lobby")
                    self.rooms[room].add(conn_id)
                    self.conn_rooms[conn_id].add(room)
                    self._send(
                        conn_id,
                        {"type": "system", "message": f"Joined room {room}"},
                    )
                    self._broadcast_room(
                        room,
                        {
                            "type": "presence",
                            "event": "join",
                            "room": room,
                            "user": user,
                        },
                    )

                elif mtype == "leave":
                    room = msg.get("room")
                    if room and conn_id in self.rooms.get(room, set()):
                        self.rooms[room].discard(conn_id)
                        self.conn_rooms[conn_id].discard(room)
                        self._broadcast_room(
                            room,
                            {
                                "type": "presence",
                                "event": "leave",
                                "room": room,
                                "user": user,
                            },
                        )

                elif mtype == "msg":
                    room = msg.get("room")
                    text = msg.get("text", "")
                    if not room or conn_id not in self.rooms.get(room, set()):
                        self._send(
                            conn_id,
                            {
                                "type": "system",
                                "message": f"Join room {room} first",
                            },
                        )
                        continue
                    self._broadcast_room(
                        room,
                        {
                            "type": "chat",
                            "room": room,
                            "user": user,
                            "text": text,
                        },
                    )

                elif mtype == "logout":
                    break

                else:
                    self._send(
                        conn_id,
                        {"type": "system", "message": "Unknown command"},
                    )
        finally:
            # cleanup on disconnect
            user = self.conn_user.get(conn_id, user or "?")
            rooms = list(self.conn_rooms.get(conn_id, set()))
            for room in rooms:
                self.rooms[room].discard(conn_id)
                self._broadcast_room(
                    room,
                    {
                        "type": "presence",
                        "event": "leave",
                        "room": room,
                        "user": user,
                    },
                )
            if conn_id in self.conn_rooms:
                del self.conn_rooms[conn_id]
            if conn_id in self.conn_user:
                del self.conn_user[conn_id]
            self.transport.close_conn(conn_id)
            print(f"Connection {conn_id} closed")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=9000)
    args = parser.parse_args()

    server = ChatServer(args.host, args.port)
    server.serve_forever()


if __name__ == "__main__":
    main()

