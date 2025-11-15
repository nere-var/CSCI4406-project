# chat_client.py — client for the chat service over UDP

import argparse 
import sys
import json
from chat_transport import ReliableUDPSocket  
import threading

def to_bytes(d: dict) -> bytes: 
    return (json.dumps(d) + '\n').encode()

def from_bytes(b: bytes) -> dict:
    return json.loads(b.decode())

class ChatClient: 
    def __init__(self, server_host: str, server_port: int):
        self.sock = ReliableUDPSocket() 
        self.sock.connect((server_host, server_port)) 
        self.sock.on_message(self._on_message)
        self.username = None
        self.check_event = threading.Event()
        self.check_result = None

    def _on_message(self, payload: bytes):
        try:
            msg = from_bytes(payload)
            cmd = msg.get('cmd')
            text = msg.get('text')
            
            if cmd == 'SYS' and text in ['username taken', 'username available', 'username exists']:
                self.check_result = text
                self.check_event.set()
                return

            if cmd == 'MSG':
                print(f"[{msg.get('room')}] {msg.get('user')}: {text}")
            elif cmd == 'SYS':
                try:
                    parsed = json.loads(text)
                    if isinstance(parsed, list):
                        print("Available chatrooms:")
                        for r in parsed:
                            print(" -", r)
                    else:
                        print(text)
                except json.JSONDecodeError:
                    print(text)
            else:
                print(payload.decode(errors='ignore'))
        except Exception:
            print(payload.decode(errors='ignore'))

    def check_user(self, user):
        self.sock.send_msg(to_bytes({'cmd': 'CHECK_USER', 'user': user}))

    def join(self, room: str):
        self.sock.send_msg(to_bytes({'cmd': 'JOIN', 'room': room, 'user': self.username}))

    def leave(self, room: str):
        self.sock.send_msg(to_bytes({'cmd': 'LEAVE', 'room': room, 'user': self.username}))

    def msg(self, room: str, text: str):
        self.sock.send_msg(to_bytes({'cmd': 'MSG', 'room': room, 'user': self.username, 'text': text}))

    def who(self, room: str):
        self.sock.send_msg(to_bytes({'cmd': 'WHO', 'room': room}))

    def register(self, user, pw):
        self.sock.send_msg(to_bytes({'cmd':'REGISTER','user':user,'pw':pw}))

    def login(self, user, pw):
        self.sock.send_msg(to_bytes({'cmd':'LOGIN','user':user,'pw':pw}))

    def list_rooms(self):
        self.sock.send_msg(to_bytes({'cmd':'LIST_ROOMS'}))

    def run(self):
        while True:
            choice = input("Register (r) or Login (l): ").strip().lower()
            if choice == 'r':
                u = None
                while True:
                    u_temp = input("New username: ").strip()
                    if not u_temp: continue
                    self.check_event.clear()
                    self.check_result = None
                    self.check_user(u_temp)
                    print("Checking username availability...")
                    if self.check_event.wait(5):
                        if self.check_result == 'username taken':
                            print("!!! Error: Username is already taken. Try again.")
                            continue
                        print(f"Username '{u_temp}' is available.")
                        u = u_temp
                        break
                    else:
                        print("!!! Error: Server took too long to respond. Cannot register.")
                        break
                if u:
                    pw = input("New password: ").strip()
                    self.register(u, pw)
                    self.username = u
                continue
            elif choice == 'l':
                u = input("Username: ").strip()
                pw = input("Password: ").strip()
                self.username = u
                self.login(u, pw)
                break

        print("Fetching available chatrooms...") 
        self.list_rooms()
        print("You may now use chat commands.")
        self.run_cli()

    def run_cli(self):
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
        self.sock.close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--server', default='127.0.0.1')
    parser.add_argument('--port', type=int, default=9000)
    args = parser.parse_args()
    ChatClient(args.server, args.port).run()
