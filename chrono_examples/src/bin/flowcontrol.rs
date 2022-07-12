use std::{
    net::{IpAddr, SocketAddr, UdpSocket},
    pin::Pin,
    thread::{sleep, spawn},
    time::{Duration, Instant},
    sync::{Arc, atomic::{AtomicUsize, Ordering}},
    path::PathBuf,
};

use clap::Parser;
use rand::random;

use sequencer_common::SequencerMessage;
use iodaemon::BlockingConsumer;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(short, long, env)]
    ip_address: IpAddr,
    #[clap(short, long, env)]
    port: u16,
    #[clap(short, long, env, value_parser = clap::value_parser!(u16).range(1..=1500))]
    size: u16,
    #[clap(long, env, default_value_t = 0)]
    interval: u64,
    #[clap(long, env, default_value_t = 0)]
    timeout: u64,
    #[clap(short, long, env, default_value_t = 1)]
    window: usize,
    #[clap(long, env)]
    path: Option<PathBuf>,
}

pub fn main() -> std::io::Result<()> {
    let args = Args::parse();

    // prepare port to send to sequencer
    let socket = UdpSocket::bind("127.0.0.1:0")?;
    socket.connect(SocketAddr::new(args.ip_address, args.port))?;

    // generate a random message payload
    let payload: Vec<u8> = (0..args.size).map(|_| random()).collect();
    let application_id = random();
    let message = SequencerMessage::new(application_id, random(), random(), 0, payload);
    let mut message_bytes = rkyv::util::to_bytes::<_, 256>(&message).unwrap();
    rkyv::check_archived_root::<SequencerMessage>(&message_bytes).unwrap();

    let last_received_arc = Arc::new(AtomicUsize::new(0));
    let mut sequence_number = 0;
    
    let _sending = spawn({ 
        let last_received = last_received_arc.clone();
        move || {
        loop {
            let archived_message =
                unsafe { rkyv::archived_root_mut::<SequencerMessage>(Pin::new(&mut message_bytes)) };

            archived_message.modify_app_sequence_number(sequence_number);
            let timer = Instant::now();
            while last_received.load(Ordering::Relaxed) + args.window < sequence_number as usize {
                // spinlock is fine for now
                if timer.elapsed() > Duration::from_millis(args.timeout) {
                    // jump back on sequence numbers to last received
                    sequence_number = last_received.load(Ordering::Relaxed) as u64;
                    continue;
                }
            }

            if socket.send(&message_bytes).is_err() {
                break;
            }
            
            sequence_number += 1;

            sleep(Duration::from_millis(args.interval)); // on Unix, doesn't even do
                                                         // a syscall with arg of 0
        }
        }
    });
    
    let receiving = spawn({ 
        let last_received = last_received_arc.clone();
        move || {
        let input_file = if args.path.is_some() {
            args.path.unwrap()
        } else {
            PathBuf::from("/var/run/iodaemon/output")
        };

        let mut reader = BlockingConsumer::new(input_file);
        
        while let Ok(message) = reader.read_message() {
            let appid = message.app_id;
            let seqno = message.app_sequence_number;
            let expected_seqno = last_received.load(Ordering::Acquire) + 1 ;
            if appid == application_id {
                if seqno == expected_seqno as u64 {
                    last_received.store(seqno as usize, Ordering::Release);
                }
                else {
                    eprintln!("WARNING: Expected sequence number {}, got {}", expected_seqno, seqno);
                }
            }
        }}
    });
    
    receiving.join().expect("Couldn't join receiver thread.");
    
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
}
