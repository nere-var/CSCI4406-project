# CSCI4406-project
## Team Info
Pixel Panthers - Adrian Lopez, Mallory Sorola, Isabel Villarreal, Emma Whitehead, Anthony Whitmore

App 2: Group Chat with Rooms + Presence

## Project Overview
### What We Will Demonstrate
- A working chat service modeled after socket programming examples including
  - usernames
  - presence notifications
  - broadcast messages to all room members
  - commands to join, leave, or send a message to a certain room
 - Reliable, in-order message delivery
 - Handling of out-of-order arrivals
 - Latency and retransmission reports
 - Implementation of Sliding Window ARQ
### Project Goals
- Building a working chat service for multiple clients
- Collect metrics like throughput, average/95th-percentile latency to produce a metrics report
- Requiring a checksum to verify message integrity
- Implementing
  - flow control
  - reliability using Selective Repeat
  - timeout and retransmission
- Ensuring our client
  - passes all lossy network shim tests without crashing or hanging
  - can handle two or more clients concurrently
  - has proper error handling for invalid commands or disconnects
## Transport Protocol Design Plan
### Reliability Protocol Choice
We plan to implement the **Selective Repeat** reliability protocol for our project versus using Go-Back-N. We've chosen to use Selective Repeat for efficiency since only lost or corrupted packets are retransmitted instead of having the entire window of packets from the lost packet to the last packet transmitted are retransmitted.
### Design Details
**Header Design:**

| Field    | Size    | Description                    |
| -------- | ------- | ------------------------------ |
| ver      | 1 byte  | protocol version               |
| flags    | 1 byte  | bits for ACK, SYN, FIN         |
| conn_id  | 2 bytes | connection ID                  |
| seq      | 4 bytes | sequence number                |
| ack      | 4 bytes | last packet received in order  |
| wnd      | 2 bytes | receiver window (flow control) |
| len      | 2 bytes | payload length                 |
| checksum | 4 bytes | CRC32 over header + data       | 

We are using CRC32 to detect accidental data corruption during transmission or storage. More info on CRC32 can be found [here](https://he3.hashnode.dev/understanding-the-crc32-hash-a-comprehensive-guide).


**Timers:**
- One timer per sent packet.
- If ACK isn’t received before timeout, resend that packet.
- Start with a fixed timeout (like 500 ms) or use adaptive timeout (measure round-trip time).
- When a packet is ACKed, stop its timer.
**Flow Control:**
- Receiver tells the sender how much buffer space it has using the wnd (window) field.
- Sender can only send up to that many bytes at once, which prevents the receiver from being overloaded.
**Retransmission Logic:**
  We will be using selective repeat to ensure retransmission for lost or corrupted packets.
### How Reliability Will Be Ensured
| Problem                | Solution                                                                                      |
| ---------------------- | --------------------------------------------------------------------------------------------- |
| **Packet loss**        | Unacknowledged packets are retransmitted after timeout.                                       |
| **Packet duplication** | Receiver uses sequence numbers to ignore duplicates.                                          |
| **Packet reordering**  | Receiver stores out-of-order packets and delivers them in order once the missing ones arrive. |
| **Corruption**         | Detected with checksum; corrupted packets are dropped and resent.                             |

## Application Layer Design Plan
### Message Format and Command Grammar
**Commands**
- JOIN \<room\> 
  - join a chatroom
- LEAVE \<room\>
  - leave a chatroom 
- MSG \<room> <text\>
  - send a message to a chatroom
- LIST
  - lists chatrooms available to join
- USERNAME REGISTER \<username> <password\>
  - allows a user to set their username and password prior to entering a chatroom for the first time 
- LOGIN \<username> <password\>
  - allows user to sign in after registering
- ONLINE \<room\>
  - lists who is online
- DISCONNECT \<room\>
  - allows user to disconnect from the chat room and return to main server screen to view list of chatrooms available or change username
### How Client and Server Will Interact
The client and server will interact by allowing a user to download the client to access a server hosted elsewhere. By having access to the client, the user can connect to the server, set a username and password, connect to a chatroom and chat, or disconnect from the server and leave the chat.\
The server will accept a sent message from a client, and broadcast it to any connected users of the chatroom. The server will also store any usernames and passwords set by users connecting by associating usernames with client sockets. It will also have a list of all users who are connected to the server, and specify which chat room they are in.
### How Concurrency Will Be Supported
Concurrency will be supported server-side by using threads. Each client connection gets its own handler that manages:
- Sending and receiving packets
- Sequence numbers
- Timers
- Buffers for Selective Repeat
When a new client connects, the server starts a new thread for that client. Each thread runs the chat logic (JOIN, MSG, LEAVE) and handles reliable transport for that connection.
## Testing and Metrics Plan
### How We Plan To Test Our System
| Profile         | Description                     | How We Simulate It                                                                                       | What We Measure                                                         |
| --------------- | ------------------------------- | -------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------- |
| **Clean**       | No packet loss or delay         | Run server and clients on the same machine or LAN with normal UDP sockets.                               | Baseline latency, throughput, and no retransmissions.                   |
| **Random Loss** | Packets are randomly dropped    | Add random `drop()` calls in code (e.g., drop 10% of packets). | Retransmissions per KB, latency increase, message delivery correctness. |
| **Bursty Loss** | Groups of packets lost together | Simulate dropping several packets in a row (e.g., 3–5 consecutive packets).                              | How fast the protocol recovers after a burst; out-of-order correction.  |

### Metrics Measured
- Latency: Time from send to receive (average and 95th percentile).
- Goodput: Messages delivered per second.
- Retransmissions: Number of packets resent per KB.
- Out-of-order: Count of packets that arrived out of order.
## Progress Summary (Midterm Status - 10/31/2025
### Implemented So Far
We have a base chat client established, where a server and client can communicate with each other. There is only support for one client at a time.
### What Remains to be Completed
We need to add the following things:
- concurrency support
- username support
- separate chat rooms
