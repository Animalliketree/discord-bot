# Discord Bot
## Summary
**THIS PROJECT HAS BEEN ARCHIVED**  
This project was a valuable learning experience with PostgreSQL and WebSocket connections. 
It is no longer being actively maintained, as I am shifting my focus toward team 
projects and graphics programming.

I will keep this repository open for reference.

This bot was designed to connect to Discord's Gateway API using WebSockets in 
Rust. Its abilities should be to receive and respond to messages, storing each 
message in a PostgreSQL database.

## Setting Up the Program
### Requirements
The program requires access to a PostgreSQL server. If you do not have a 
server already, don't worry! There will be instructions on setting up a 
PostgreSQL server during setup.

Any required packages will automatically be downloaded via Rust's `cargo` when you 
build the program.

## Running the Program
1. Start the PostgreSQL server and ensure it can be connected to.
2. Run the program using `cargo run`.

## Using the Program
There are currently no tests for the program.
