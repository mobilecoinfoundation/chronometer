#![feature(io_error_more)]

use std::{
    net::{IpAddr, SocketAddr, UdpSocket},
    pin::{Pin, self},
    thread::{sleep, spawn},
    time::{Duration, Instant},
    sync::{Arc, atomic::{AtomicUsize, Ordering}},
    path::PathBuf, process::Command,
};

use clap::Parser;
use rand::random;

use sequencer_common::{SequencerMessage, Timestamp};
use iodaemon::BlockingConsumer;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(short, long, env)]
    ip_address: IpAddr,
    #[clap(short, long, env)]
    port: u16,
    #[clap(short, long, env, value_parser = clap::value_parser!(u16).range(1..=1484))]
    size: u16,
    /// How many times should we ping the server to estimate RTT 
    #[clap(long, env, value_parser = clap::value_parser!(u16).range(1..=1484), default_value_t = 25)]
    pub ping_count: u16,
    #[clap(long, env, default_value_t = 0)]
    interval: u64,
    #[clap(long, env, default_value_t = 0)]
    timeout: u64,
    #[clap(short, long, env, default_value_t = 1)]
    window: usize,
    #[clap(long, env)]
    path: Option<PathBuf>,
}

#[derive(Debug, Default, Clone)]
struct Payload {
    /// What is the most recent timestamp we've gotten back from the server, before sending this message?
    pub our_time_sent: Timestamp,
    /// Additional bytes to pad out the message and make it longer. 
    pub extra_bytes: Vec<u8>, 
}


pub fn main() -> std::io::Result<()> {
    let args = Args::parse();

    // prepare port to send to sequencer
    let socket = UdpSocket::bind("0.0.0.0:0")?;
    socket.connect(SocketAddr::new(args.ip_address, args.port))?;

    // Estimate RRTI

    let ping_count_str = args.ping_count.to_string();
    let address_str = args.ip_address.to_string();

    println!("ping {:?}", [&address_str, "-c", &ping_count_str]);

    let ping_output = Command::new("ping")
            .args([&address_str, "-c", &ping_count_str])
            .output()
            .expect("failed to execute process");

    if !ping_output.status.success() {
        return Err(std::io::Error::new(std::io::ErrorKind::HostUnreachable,  format!("Error pinging server: {}", String::from_utf8_lossy(&ping_output.stdout))));
    }

    let ping_stdout = String::from_utf8_lossy(&ping_output.stdout).to_string();
    let ping_results_intermediary = ping_stdout.strip_suffix(" ms\n").unwrap();
    let ping_results_intermediary = ping_results_intermediary.rsplit_once("rtt min/avg/max/mdev = ").unwrap().1;
    println!("{}", ping_results_intermediary);

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
