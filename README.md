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
    
## Transport Protocol Design 
Header Structure
Each packet contains :
- Versions
- Flags
- Conn_ID, which is connection identifier
- Sequence number for data packets
- Acknowledgement value
- Payload length
- Checksum - error detection

### Reliability Protocol Choice
We plan to implement the **Selective Repeat** reliability protocol for our project versus using Go-Back-N. We've chosen to use Selective Repeat for efficiency since only lost or corrupted packets are retransmitted instead of having the entire window of packets from the lost packet to the last packet transmitted are retransmitted.
 - To implement reliability, we used Selective Repeat ARQ, which is one of the allowed sliding windows ARQ protocols.
Selective repeat works by:
- Allowing multiple packets to be unacknowledged at once
- Retransmitting only packets that were lost
- Buffering out of order packets
- Delivering to the app layer once all earlier packets arrive

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
- Each outgoing packet receives its own independent timer. 
- On a timeout, that specific packet is retransmitted and the timer is restarted.
- Unaffected packets continue as normal
- When retransmitted the window does NOT reset

**Flow Control:**
  - Flow Control is implemented using the receiver-advertised sliding window, stored in the wnd header field. 
  - Receiver infors sender how many packets can be safely accepted, while sender limits outstanding packets.

**Checksum:**
- All packets include a checksum that covers a header and payload. 
- When the checksum verification fails,  the incorrect packet is discarded and naturally triggers selective repeat transmission.
  
**Clean Application-Layer API
Our transport layer provides a clean interface used by the chat application: 
- connect(addr)
- send_msg(b)
- on_message(callback)
- close()



**Retransmission Logic:**
  We will be using selective repeat to ensure retransmission for lost or corrupted packets.
### How Reliability Will Be Ensured
| Problem                | Solution                                                                                      |
| ---------------------- | --------------------------------------------------------------------------------------------- |
| **Packet loss**        | Unacknowledged packets are retransmitted after timeout.                                       |
| **Packet duplication** | Receiver uses sequence numbers to ignore duplicates.                                          |
| **Packet reordering**  | Receiver stores out-of-order packets and delivers them in order once the missing ones arrive. |
| **Corruption**         | Detected with checksum; corrupted packets are dropped and resent.                             |

## Application Layer Design ##
Our application uses a simple, slash-prefixed command grammar that keeps client interaction intuitive, while also remaining simple for the server to parse. Some features provided in our design are: 
- Presence Notifications, where presence updates occur when a client joins, leaves, or disconnects unexpectedly from a room.
- Error Handling, where we implemented a robust error handling for unknown commands, malformed arguments, invalid login credentials,     nonexistent rooms, unauthorized actions, and unexpected client disconnect.
- Concurrency Model, where the server uses a multi-threaded design for one thread per connected client, shared data structures like       rooms, and user tables, and properlocking to prevent race conditions. 





### Message Format and Command Grammar
**Commands**
- JOIN \<room\> 
  - join a chatroom
- LEAVE \<room\>
  - leave a chatroom 
- MSG \<room> <text\>
  - send a message to a chatroom
- DM USER <text>
  - lists chatrooms available to join
- /quit \<room\>
  - allows user to disconnect from the chat room and return to main server screen to view list of chatrooms available or change username
### How Client and Server Will Interact
The client and server will interact by allowing a user to download the client to access a server hosted elsewhere. By having access to the client, the user can connect to the server, set a username and password, connect to a chatroom and chat, or disconnect from the server and leave the chat.\
The server will accept a sent message from a client, and broadcast it to any connected users of the chatroom. The server will also store any usernames and passwords set by users connecting by associating usernames with client sockets. It will also have a list of all users who are connected to the server, and specify which chat room they are in.

### Client ###
- The user is required to register an account with a unique username and password
- Once they’ve registered, they can login with those same credentials
- Upon login the user gets access to more features:
- Join a room of their choice and chat
- Privately direct message (DM) other users
- View message history
- Leave the room and join another
- Quit

### Server ###
- Listens on one port (default UDP 9000)
- Accepts connections from multiple clients
- Authenticates users (register/login)
- Hosts multiple chat rooms
- Broadcasts messages to everyone in the same room
- Maintains active user lists
- Saves credentials to disk (accounts.json)


### Transport ###

- Guarantees messages arrive and in the right order
- Creates connections between client and server
- Resends lost data automatically
- Delivers messages to chat_client.py
- Tracks performance metrics
- CRC32 is used to detect corruption




### How Concurrency Will Be Supported
Concurrency will be supported server-side by using threads. Each client connection gets its own handler that manages:
- Sending and receiving packets
- Sequence numbers
- Timers
- Buffers for Selective Repeat
When a new client connects, the server starts a new thread for that client. Each thread runs the chat logic (JOIN, MSG, LEAVE) and handles reliable transport for that connection.
## Testing and Metrics 
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
8
