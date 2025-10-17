# chat_client.py —  client for the chat service over our UDP transport

import argparse
import sys
import json
from chat_transport import ReliableUDPSocket  

def to_bytes(d: dict) -> bytes:
    """Encode dict to JSON bytes (simple text protocol)."""
    return (json.dumps(d) + '\n').encode()

def from_bytes(b: bytes) -> dict:
    """Decode JSON bytes to dict."""
    return json.loads(b.decode())


class ChatClient:
    def __init__(self, server_host: str, server_port: int, username: str):
        """Create reliable UDP socket, connect to server, register RX callback, store username."""
        self.sock = ReliableUDPSocket()
        self.sock.connect((server_host, server_port))
        self.sock.on_message(self._on_message)
        self.username = username

    def _on_message(self, payload: bytes):
        """Handle inbound messages: print chat lines and system notifications."""
        try:
            msg = from_bytes(payload)
            if msg.get('cmd') == 'MSG':
                print(f"[{msg.get('room')}] {msg.get('user')}: {msg.get('text')}")
            elif msg.get('cmd') == 'SYS':
                print(msg.get('text'))
            else:
                # Unknown type; show raw
                print(payload.decode(errors='ignore'))
        except Exception:
            # Decoding failed; show raw
            print(payload.decode(errors='ignore'))

    # --- Command helpers (send JSON commands over transport) ---

    def join(self, room: str):
        """JOIN <room>: subscribe to a room and trigger presence notice."""
        self.sock.send_msg(to_bytes({'cmd': 'JOIN', 'room': room, 'user': self.username}))

    def leave(self, room: str):
        """LEAVE <room>: unsubscribe from a room and trigger presence notice."""
        self.sock.send_msg(to_bytes({'cmd': 'LEAVE', 'room': room, 'user': self.username}))

    def msg(self, room: str, text: str):
        """MSG <room> <text>: broadcast a chat message to all members of the room."""
        self.sock.send_msg(to_bytes({'cmd': 'MSG', 'room': room, 'user': self.username, 'text': text}))

    def who(self, room: str):
        """WHO <room>: ask server to list current members of the room."""
        self.sock.send_msg(to_bytes({'cmd': 'WHO', 'room': room}))

    def run_cli(self):
        """Simple REPL: reads one line at a time and dispatches commands."""
        print("Commands: JOIN <room> | LEAVE <room> | MSG <room> <text> | WHO <room> | QUIT")
        for line in sys.stdin:
            parts = line.strip().split(' ', 2)
            if not parts or parts[0] == '':
                continue
            cmd = parts[0].upper()
            if cmd == 'JOIN' and len(parts) >= 2:
                self.join(parts[1])
            elif cmd == 'LEAVE' and len(parts) >= 2:
                self.leave(parts[1])
            elif cmd == 'MSG' and len(parts) >= 3:
                self.msg(parts[1], parts[2])
            elif cmd == 'WHO' and len(parts) >= 2:
                self.who(parts[1])
            elif cmd == 'QUIT':
                break
        self.sock.close()  # graceful close


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--server', default='127.0.0.1')
    parser.add_argument('--port', type=int, default=9000)
    parser.add_argument('--user', required=True)
    args = parser.parse_args()

    ChatClient(args.server, args.port, args.user).run_cli()
