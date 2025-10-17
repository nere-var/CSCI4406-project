# chat_server.py — Midterm server with rooms + presence over our UDP transport

import argparse
import threading
from collections import defaultdict
from chat_transport import ReliableUDPSocket  
import json

def to_bytes(d: dict) -> bytes:
    return (json.dumps(d) + '\n').encode()

def from_bytes(b: bytes) -> dict:
    return json.loads(b.decode())


class ChatServer:
    def __init__(self, host='0.0.0.0', port=9000):
        """Bind reliable UDP socket; set up room & username maps; register RX callback."""
        self.sock = ReliableUDPSocket(local_addr=(host, port))
        self.rooms = defaultdict(set)   # room to set(peer_addr)
        self.usernames = {}             # peer_addr to username
        self._lock = threading.RLock()
        self.sock.on_message(self._on_message)

    def _broadcast(self, room: str, payload: bytes, exclude=None):
        """Send payload to all peers in a room """
        for peer in list(self.rooms[room]):   # snapshot so we can modify set safely
            if exclude and peer == exclude:
                continue
            self.sock.peer = peer
            self.sock.send_msg(payload)

    def _send_peer(self, peer, payload: bytes):
        """Send payload to one peer."""
        self.sock.peer = peer
        self.sock.send_msg(payload)

    def _on_message(self, payload: bytes):
        """Handle JOIN/LEAVE/MSG/WHO from the actual sender (last_from)."""
        msg = from_bytes(payload)

        # *** IMPORTANT: use the real sender of this packet ***
        peer = self.sock.last_from
        if not peer:
            return

        cmd = (msg.get('cmd') or '').upper()
        room = msg.get('room')
        text = msg.get('text')
        user = msg.get('user') or f"{peer[0]}:{peer[1]}"

        with self._lock:
            if cmd == 'JOIN' and room:
                # add sender to room; remember username
                self.usernames[peer] = user
                self.rooms[room].add(peer)

                # presence to everyone in the room (including the joiner)
                self._broadcast(room, to_bytes({'cmd':'SYS',
                                               'text':f'[presence] {user} joined {room}'}))

            elif cmd == 'LEAVE' and room:
                if peer in self.rooms[room]:
                    self.rooms[room].remove(peer)
                    self._broadcast(room, to_bytes({'cmd':'SYS',
                                                    'text':f'[presence] {user} left {room}'}))

            elif cmd == 'MSG' and room and text is not None:
                # fan out chat message
                self._broadcast(room, to_bytes({'cmd':'MSG',
                                                'room':room, 'user':user, 'text':text}))

            elif cmd == 'WHO' and room:
                users = [self.usernames.get(p, str(p)) for p in self.rooms[room]]
                self._send_peer(peer, to_bytes({'cmd':'SYS',
                                                'text':f"members[{room}]: {', '.join(users)}"}))

            else:
                self._send_peer(peer, to_bytes({'cmd':'SYS',
                                                'text':'unrecognized or malformed command'}))

    def serve_forever(self):
        print("Chat server (midterm) running...")
        try:
            while True:
                threading.Event().wait(1)
        except KeyboardInterrupt:
            pass


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', default='0.0.0.0')
    parser.add_argument('--port', type=int, default=9000)
    args = parser.parse_args()

    ChatServer(args.host, args.port).serve_forever()

