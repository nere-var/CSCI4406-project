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
We plan to implement the **Selective Repeat** reliability protocol for our project versus using Go-Back-N. We've chosen to use Selective Repeat for efficiency since only lost or error packets are retransmitted instead of having the entire window of packets from the lost packet to the last packet transmitted are retransmitted.
### Design Details
Design details such as header fields, timers, flow control, and retransmission logic.
### How Reliability Will Be Ensured
How your implementation will ensure reliability and handle packet loss, duplication, or reordering.
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
- USERNAME CHANGE \<old username> <password> <new username\> 
  - allows a user to change their username
- LOGIN \<username> <password\>
  - allows user to sign in after registering
- ONLINE \<room\>
  - lists who is online
### How Client and Server Will Interact
The client and server will interact by allowing a user to download the client to access a server hosted elsewhere. By having access to the client, the user can connect to the server, set a username and password, connect to a chatroom and chat, change their username, or disconnect from the server and leave the chat.\
The server will accept a sent message from a client, and broadcast it to any connected users of the chatroom. The server will also store any usernames and passwords set by users connecting by associating usernames with client sockets. It will also have a list of all users who are connected to the server, and specify which chat room they are in.
### How Concurrency Will Be Supported
How concurrency will be supported (at least 2 clients).
## Testing and Metrics Plan
### How We Plan To Test Our System
How you plan to test your system under the three lossy network profiles (Clean, Random Loss, Bursty Loss).
### Metrics Measured
Which metrics you intend to measure (e.g., throughput, latency, retransmissions, dropped frames, stall time).
## Progress Summary (Midterm Status - 10/31/2025
### Implemented So Far
What has been implemented so far (with brief descriptions of working components).
### What Remains to be Completed
What remains to be completed for the final milestone.
Evidence of progress such as code structure, working prototypes, or initial testing.
