use std::{
    net::{IpAddr, SocketAddr, UdpSocket},
    pin::Pin,
    thread::sleep,
    time::Duration,
};

use clap::Parser;
use rand::random;

use sequencer_common::SequencerMessage;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(short, long, env)]
    ip_address: IpAddr,
    #[clap(short, long, env)]
    port: u16,
    #[clap(short, long, env, value_parser = clap::value_parser!(u16).range(1..1500))]
    size: u16,
    #[clap(long, env, default_value_t = 0)]
    interval: u64,
}

pub fn main() -> std::io::Result<()> {
    let args = Args::parse();

    // prepare port to send to sequencer
    let socket = UdpSocket::bind("127.0.0.1:0")?;
    socket.connect(SocketAddr::new(args.ip_address, args.port))?;

    // generate a random message payload
    let payload: Vec<u8> = (0..args.size).map(|_| random()).collect();
    let message = SequencerMessage::new(random(), random(), random(), 0, payload);
    let mut message_bytes = rkyv::util::to_bytes::<_, 256>(&message).unwrap();
    rkyv::check_archived_root::<SequencerMessage>(&message_bytes).unwrap();

    loop {
        let archived_message =
            unsafe { rkyv::archived_root_mut::<SequencerMessage>(Pin::new(&mut message_bytes)) };

        let seqno = archived_message.app_sequence_number;
        archived_message.modify_app_sequence_number(seqno + 1);

        socket.send(&message_bytes)?;

        sleep(Duration::from_millis(args.interval)); // on Unix, doesn't even do
                                                     // a syscall with arg of 0
    }
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
