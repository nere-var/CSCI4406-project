import argparse
import json
from typing import Optional
from chat_transport import ReliableUDPSocket

HISTORY_LIMIT = 20


class ChatServer:
    def __init__(self, port: int):
        self.port = port

        # Reliable UDP socket
        self.rudp = ReliableUDPSocket(listen_addr=("0.0.0.0", port), is_server=True)

        # conn_id -> username
        self.usernames = {}

        # username -> conn_id
        self.user_to_conn = {}

        # room -> set of usernames
        self.rooms = {"general": set()}

        # room -> list of past messages (text only, last HISTORY_LIMIT)
        self.history = {"general": []}

        # load accounts
        self.accounts = self._load_accounts()
        print(f"Loaded {len(self.accounts)} accounts.")

    # -----------------------
    # ACCOUNT HANDLING
    # -----------------------

    def _load_accounts(self):
        try:
            with open("accounts.json", "r") as f:
                return json.load(f)
        except Exception:
            return {}

    def _save_accounts(self):
        with open("accounts.json", "w") as f:
            json.dump(self.accounts, f, indent=2)

    # -----------------------
    # SERVER MAIN LOOP
    # -----------------------

    def run(self):
        print(f"Chat server running on UDP port {self.port}...")
        import threading
        while True:
            try:
                conn_id, addr = self.rudp.accept(timeout=None)
            except TimeoutError:
                continue
            # one handler thread per connection
            threading.Thread(target=self._client_handler, args=(conn_id,), daemon=True).start()

    # -----------------------
    # UTILITIES
    # -----------------------

    def _send(self, conn_id: int, data: dict) -> None:
        """Shortcut for JSON send."""
        self.rudp.send_json(conn_id, data)

    def _broadcast(self, room: str, data: dict) -> None:
        """Send a JSON message to all users in a room."""
        for username in list(self.rooms.get(room, [])):
            cid = self.user_to_conn.get(username)
            if cid is not None:
                self._send(cid, data)

    def _add_history(self, room: str, text: str) -> None:
        hist = self.history.setdefault(room, [])
        hist.append(text)
        if len(hist) > HISTORY_LIMIT:
            hist.pop(0)

    def _send_history_to_user(self, conn_id: int, room: str) -> None:
        for msg in self.history.get(room, []):
            self._send(conn_id, {
                "type": "history",
                "room": room,
                "text": msg,
            })

    # -----------------------
    # CLIENT HANDLER
    # -----------------------

    def _client_handler(self, conn_id: int) -> None:
        # Authenticate first
        username = self._handle_auth(conn_id)
        if username is None:
            return

        # Track username
        self.usernames[conn_id] = username
        self.user_to_conn[username] = conn_id

        # Auto-join general
        self._join_room(username, conn_id, "general")

        # Main loop
        while True:
            msg = self.rudp.recv_json(conn_id, timeout=1.0)
            if msg is None:
                continue

            if msg.get("type") == "logout":
                self._handle_logout(username, conn_id)
                return

            mtype = msg.get("type")

            if mtype == "join":
                self._join_room(username, conn_id, msg.get("room"))

            elif mtype == "leave":
                self._leave_room(username, conn_id, msg.get("room"))

            elif mtype == "msg":
                room = msg.get("room")
                text = msg.get("text")
                self._handle_room_message(username, room, text)

            elif mtype == "dm":
                dest = msg.get("to")
                text = msg.get("text")
                self._handle_dm(username, dest, text)

    # -----------------------
    # AUTHENTICATION
    # -----------------------

    def _handle_auth(self, conn_id: int) -> Optional[str]:
        while True:
            msg = self.rudp.recv_json(conn_id, timeout=None)
            if not msg:
                continue

            if msg.get("type") != "auth":
                self._send(conn_id, {"type": "auth_error", "message": "Expected auth."})
                continue

            mode = msg.get("mode")
            username = msg.get("username")
            password = msg.get("password")

            # register
            if mode == "register":
                if username in self.accounts:
                    self._send(conn_id, {
                        "type": "auth_error",
                        "message": "Username already exists.",
                    })
                    continue

                self.accounts[username] = password
                self._save_accounts()

                self._send(conn_id, {
                    "type": "auth_ok",
                    "message": "Registered successfully.",
                })
                return username

            # login
            if mode == "login":
                if username in self.accounts and self.accounts[username] == password:
                    self._send(conn_id, {
                        "type": "auth_ok",
                        "message": "Login successful.",
                    })
                    return username

                self._send(conn_id, {
                    "type": "auth_error",
                    "message": "Invalid credentials.",
                })

    # -----------------------
    # ROOM HANDLERS
    # -----------------------

    def _join_room(self, username: str, conn_id: int, room: Optional[str]) -> None:
        if not room:
            return
        room = room.lower()

        # Create room if needed
        self.rooms.setdefault(room, set())
        self.history.setdefault(room, [])

        self.rooms[room].add(username)

        # Send history
        self._send_history_to_user(conn_id, room)

        # Notify room
        self._broadcast(room, {
            "type": "presence",
            "event": "join",
            "room": room,
            "user": username,
        })

    def _leave_room(self, username: str, conn_id: int, room: Optional[str]) -> None:
        if not room:
            return
        room = room.lower()
        if room not in self.rooms:
            return

        if username in self.rooms[room]:
            self.rooms[room].remove(username)

            self._broadcast(room, {
                "type": "presence",
                "event": "leave",
                "room": room,
                "user": username,
            })

    # -----------------------
    # ROOM MESSAGES
    # -----------------------

    def _handle_room_message(self, username: str, room: Optional[str], text: Optional[str]) -> None:
        if not room or text is None:
            return

        room = room.lower()
        if room not in self.rooms:
            return

        # Add to history
        self._add_history(room, f"{username}: {text}")

        # Broadcast
        self._broadcast(room, {
            "type": "chat",
            "room": room,
            "user": username,
            "text": text,
        })

    # -----------------------
    # PRIVATE MESSAGES
    # -----------------------

    def _handle_dm(self, src: str, dest: Optional[str], text: Optional[str]) -> None:
        if dest is None or text is None:
            return

        dest_conn = self.user_to_conn.get(dest)
        src_conn = self.user_to_conn.get(src)

        if not dest_conn:
            # Tell sender the user isn't online
            if src_conn:
                self._send(src_conn, {
                    "type": "system",
                    "message": f"User '{dest}' not online.",
                })
            return

        # Send to receiver
        self._send(dest_conn, {
            "type": "dm",
            "from": src,
            "text": text,
        })

        # Confirmation to sender
        if src_conn:
            self._send(src_conn, {
                "type": "dm_sent",
                "to": dest,
                "text": text,
            })

    # -----------------------
    # LOGOUT
    # -----------------------

    def _handle_logout(self, username: str, conn_id: int) -> None:
        # remove from all rooms
        for room_name, room in self.rooms.items():
            if username in room:
                room.remove(username)
                # broadcast leave for that room
                self._broadcast(room_name, {
                    "type": "presence",
                    "event": "leave",
                    "room": room_name,
                    "user": username,
                })

        # cleanup maps
        self.user_to_conn.pop(username, None)
        self.usernames.pop(conn_id, None)

        print(f"{username} disconnected.")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=9000)
    args = parser.parse_args()

    srv = ChatServer(args.port)
    srv.run()


if __name__ == "__main__":
    main()
