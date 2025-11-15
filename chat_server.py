# chat_server.py — server with rooms and usernames

import argparse
import threading
from collections import defaultdict
from chat_transport import ReliableUDPSocket  
import json
import os 

def to_bytes(d: dict) -> bytes:
    return (json.dumps(d) + '\n').encode()

def from_bytes(b: bytes) -> dict:
    return json.loads(b.decode())

ACCOUNTS_FILE = 'accounts.json' # stores usernames and passwords

class ChatServer:
    def __init__(self, host='0.0.0.0', port=9000):
        self.sock = ReliableUDPSocket(local_addr=(host, port))
        self.rooms = defaultdict(set)   # room -> set(peer_addr)
        self.usernames = {}             # peer_addr -> username
        self.active_room = {}           # peer_addr -> current room
        self.accounts_file = ACCOUNTS_FILE
        self.accounts = {}              # username -> password
        self.logged_in = {}             # peer_addr -> username

        # Precreate rooms
        for room in ["general", "school", "music", "games"]:
            self.rooms[room] = set()

        self._lock = threading.RLock()
        self.sock.on_message(self._on_message)

        self._load_accounts()

    # Load accounts on startup
    def _load_accounts(self):
        if os.path.exists(self.accounts_file):
            try:
                with open(self.accounts_file, 'r') as f:
                    self.accounts = json.load(f)
                print(f"Loaded {len(self.accounts)} user accounts from {self.accounts_file}")
            except Exception as e:
                print(f"Error loading accounts: {e}")
                self.accounts = {}

    # Save accounts to file
    def _save_accounts(self):
        with open(self.accounts_file, 'w') as f:
            json.dump(self.accounts, f, indent=4)
        print(f"Saved {len(self.accounts)} user accounts to {self.accounts_file}")

    # Send to all in room
    def _broadcast(self, room: str, payload: bytes, exclude=None):
        for peer in list(self.rooms[room]):
            if exclude and peer == exclude:
                continue
            self.sock.peer = peer
            self.sock.send_msg(payload)

    # Send to a single peer
    def _send_peer(self, peer, payload: bytes):
        self.sock.peer = peer
        self.sock.send_msg(payload)

    # Handle messages from clients
    def _on_message(self, payload: bytes):
        try:
            msg = from_bytes(payload)
        except Exception:
            return
        peer = self.sock.last_from
        if not peer:
            return

        cmd = (msg.get('cmd') or '').upper()
        room = msg.get('room')
        text = msg.get('text')
        user = msg.get('user') or f"{peer[0]}:{peer[1]}"

        # --- REGISTER / CHECK_USER ---
        if cmd == 'CHECK_USER':
            u = msg.get('user')
            if not u:
                self._send_peer(peer, to_bytes({'cmd':'SYS','text':'missing username'}))
            elif u in self.accounts:
                self._send_peer(peer, to_bytes({'cmd':'SYS','text':'username taken'}))
            else:
                self._send_peer(peer, to_bytes({'cmd':'SYS','text':'username available'}))
            return

        if cmd == 'REGISTER':
            u = msg.get('user')
            pw = msg.get('pw')
            with self._lock:
                if not u or not pw:
                    self._send_peer(peer, to_bytes({'cmd':'SYS','text':'missing credentials'}))
                elif u in self.accounts:
                    self._send_peer(peer, to_bytes({'cmd':'SYS','text':'username exists'}))
                else:
                    self.accounts[u] = pw
                    self._save_accounts()
                    self._send_peer(peer, to_bytes({'cmd':'SYS','text':'registered'}))
            return

        if cmd == 'LOGIN':
            u = msg.get('user')
            pw = msg.get('pw')
            if not u or not pw or u not in self.accounts or self.accounts[u] != pw:
                self._send_peer(peer, to_bytes({'cmd':'SYS','text':'invalid login'}))
            else:
                with self._lock:
                    self.logged_in[peer] = u
                    self.usernames[peer] = u
                self._send_peer(peer, to_bytes({'cmd':'SYS','text':'login ok'}))
            return

        if cmd == 'LIST_ROOMS':
            rooms = list(self.rooms.keys())
            self._send_peer(peer, to_bytes({'cmd':'SYS','text':json.dumps(rooms)}))
            return

        # --- Require login for normal commands ---
        if peer not in self.logged_in:
            self._send_peer(peer, to_bytes({'cmd':'SYS','text':'login required'}))
            return
        user = self.logged_in[peer]

        with self._lock:
            if cmd == 'JOIN' and room:
                self.usernames[peer] = user
                self.rooms[room].add(peer)
                self.active_room[peer] = room
                self._broadcast(room, to_bytes({'cmd':'SYS',
                                               'text':f'[presence] {user} joined {room}'}))
            elif cmd == 'LEAVE' and room:
                if peer in self.rooms[room]:
                    self.rooms[room].remove(peer)
                    if self.active_room.get(peer) == room:
                        self.active_room.pop(peer)
                    self._broadcast(room, to_bytes({'cmd':'SYS',
                                                    'text':f'[presence] {user} left {room}'}))
            elif cmd == 'MSG':
                if not room:
                    room = self.active_room.get(peer)
                if room and text is not None:
                    self._broadcast(room, to_bytes({'cmd':'MSG',
                                                    'room':room, 'user':user, 'text':text}))
            elif cmd == 'WHO' and room:
                users = [self.usernames.get(p, str(p)) for p in self.rooms[room]]
                self._send_peer(peer, to_bytes({'cmd':'SYS',
                                                'text':f"members[{room}]: {', '.join(users)}"}))
            else:
                self._send_peer(peer, to_bytes({'cmd':'SYS','text':'unrecognized or malformed command'}))

    # Main server loop
    def serve_forever(self):
        print("Chat server running...")
        try:
            while True:
                threading.Event().wait(1)
        except KeyboardInterrupt:
            print("Server shutting down...")
            self._save_accounts()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', default='0.0.0.0')
    parser.add_argument('--port', type=int, default=9000)
    args = parser.parse_args()
    ChatServer(args.host, args.port).serve_forever()
