# CSCI4406-project
## Team Info
Pixel Panthers - Adrian Lopez, Mallory Sorola, Isabel Villarreal, Emma Whitehead, Anthony Whitmore

App 2: Group Chat with Rooms + Presence

## Project Overview
#### **What We Will Demonstrate**
- A working chat service modeled after socket programming examples including
  - usernames
  - presence notifications
  - broadcast messages to all room members
  - commands to join, leave, or send a message to a certain room
 - Reliable, in-order message delivery
 - Handling of out-of-order arrivals
 - Latency and retransmission reports
 - Implementation of Sliding Window ARQ
#### **Project Goals**
- Building a working chat service for multiple clients
- Collect metrics like throughput, average/95th-percentile latency to produce a metrics report
- Requiring a checksum to verify message integrity
- Implementing
  - flow control
  - reliability using Go-Back-N or Selective Repeat
  - timeout and retransmission
- Ensuring our client
  - passes all lossy network shim tests without crashing or hanging
  - can handle two or more clients concurrently
  - has proper error handling for invalid commands or disconnects
