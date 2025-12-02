#!/usr/bin/env python3
import argparse
import os
import random
import subprocess
import sys
import time
import socket
import threading

# Network profiles
PROFILES = {
    "clean": {
        "loss": 0.0,
        "dup": 0.0,
        "delay": 0.0,
        "burst_loss": 0.0,
    },
    "random": {
        "loss": 0.10,       # 10% random loss
        "dup": 0.03,        # 3% duplicates
        "delay": 0.03,      # small random delay
        "burst_loss": 0.0,
    },
    "bursty": {
        "loss": 0.05,
        "dup": 0.02,
        "delay": 0.03,
        "burst_loss": 0.25, # 25% chance to drop 3–6 packets in a row
    },
}

def forwarder(sock_in, sock_out, profile):
    while True:
        packet, addr = sock_in.recvfrom(65535)

        # Random loss
        if random.random() < profile["loss"]:
            continue

        # Bursty loss
        if profile["burst_loss"] > 0 and random.random() < profile["burst_loss"]:
            # drop 3–6 consecutive packets
            for _ in range(random.randint(3, 6)):
                try:
                    sock_in.recvfrom(65535)
                except:
                    break
            continue

        # Duplicate packets
        if random.random() < profile["dup"]:
            sock_out.send(packet)

        # Delay
        if profile["delay"] > 0:
            time.sleep(random.uniform(0, profile["delay"]))

        # Forward normally
        sock_out.send(packet)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", choices=PROFILES.keys(), default="clean")
    parser.add_argument("cmd", nargs=argparse.REMAINDER)
    args = parser.parse_args()

    if not args.cmd:
        print("Usage: lossy_net.py --profile PROFILE -- python3 chat_server.py ...")
        sys.exit(1)

    profile = PROFILES[args.profile]
    print(f"[lossy_net] Starting with profile '{args.profile}'")
    print(f"[lossy_net] Parameters: {profile}")

    # Create two UDP sockets (inbound and outbound)
    sock_a = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock_b = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    sock_a.bind(("127.0.0.1", 9001))
    sock_b.bind(("127.0.0.1", 9002))

    # Forward A→B and B→A in two threads
    t1 = threading.Thread(target=forwarder, args=(sock_a, sock_b, profile), daemon=True)
    t2 = threading.Thread(target=forwarder, args=(sock_b, sock_a, profile), daemon=True)
    t1.start()
    t2.start()

    # Run the server/client command
    cmd = args.cmd
    os.execvp(cmd[0], cmd)


if __name__ == "__main__":
    main()
