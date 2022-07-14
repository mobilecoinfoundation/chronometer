//! A simple echo server which takes in packets on a Rust stdlib UDP socket and sends them 
//! right back out on a TCP socket managed by Tokio, and that's it. 
//! This is used as the "control group" of roundtrip_benchmark 
//! to measure the time on the wire, so that we can determine how much time is actually 
//! spent doing sequencer logic vs how much round-trip time is required for packets to reach
//! their destination and for the reply to return.


pub fn main() -> std::io::Result<()> {

    Ok(())
}