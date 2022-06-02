use bytecheck::StructCheckError;
use clap::Parser;
use rkyv::validation::{CheckArchiveError, validators::DefaultValidatorError};
use std::{net::{Ipv6Addr, SocketAddrV6}, fmt::Display};
use std::io::ErrorKind as IoErrorKind;
use tokio::{self, net::UdpSocket};
use unsequenced_producer::{UnsequencedInput, ArchivedUnsequencedInput};

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(short, long, default_value_t = 1)]
    instance: u16,
    #[clap(short, long, default_value_t = 0)]
    port: u16,
}

pub type ReadArchiveError = CheckArchiveError<StructCheckError, DefaultValidatorError>; 
#[derive(Debug)]
pub enum SequencerReadError { 
    /// The sequencer could not bind the local socket required to read incoming UDP packets.
    UdpBindError(u16, std::io::Error),
    /// socket.recv_from() failed
    UdpReceiveError(std::io::Error),
    /// A message was received for which the UnsequencedInput header was invalid and could not be read by rkyv.
    /// We have to first format the underlying error to a string because a CheckArchiveError is not Send
    CouldNotParse(String),
}

impl Display for SequencerReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SequencerReadError::UdpBindError(port, source_error) => write!(f, "Could not bind a UDP socket on port {}: {:?}", port, source_error),
            SequencerReadError::UdpReceiveError(source_error) => write!(f, "IO error while reading from UDP socket: {:?}", source_error),
            SequencerReadError::CouldNotParse(source_error) => write!(f, "Unable to parse an incoming UnsequencedInput header: {:?}", source_error),
        }
    }
}

impl std::error::Error for SequencerReadError {}

impl From<ReadArchiveError> for SequencerReadError {
    fn from(e: ReadArchiveError) -> Self {
        Self::CouldNotParse( format!("{:?}", e) )
    }
}

struct InboundServer {
    port: u16,
}

impl InboundServer {
    pub fn new(port: u16) -> InboundServer {
        InboundServer { port }
    }

    pub async fn start<F>(self, closure: F) -> std::result::Result<(), SequencerReadError>
        where F: Fn(&ArchivedUnsequencedInput) {
        let addr = SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, self.port, 0, 0);
        let listener = UdpSocket::bind(addr).await
            .map_err(|e| SequencerReadError::CouldNotParse(format!("{:?}", e)))?;

        println!("Hello, world on port {:?}!", listener);
        let mut recv_buffer = vec![0u8; 2048];
        loop {
            let (len, addr) = listener.recv_from(&mut recv_buffer).await
                .map_err(SequencerReadError::UdpReceiveError)?;
            println!("{}, {:?}", len, addr);
            let data_res = rkyv::check_archived_root::<UnsequencedInput>(&recv_buffer[..len]);

            match data_res {
                Ok(data) => {
                    closure(data);
                }
                Err(e) => {
                    return Err(e.into());
                }
            }
        }
    }
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let args = Args::parse();

    let inbound_server = InboundServer::new(args.port);
    let join_handle = tokio::spawn(inbound_server.start(|message| println!("Received: {:?}", message) ));

    join_handle.await?.map_err(|e| std::io::Error::new(IoErrorKind::InvalidData, Box::new(e)) )
}

#[tokio::test(flavor = "multi_thread")]
async fn test_stream_received() {
    use rkyv::Deserialize;
    use std::time::Duration;
    use tokio::time::timeout;

    // Dummy message for testing. 
    let example_message = "Hello, world!";
    let payload = example_message.as_bytes().to_vec();
    // Build an archive
    let input = UnsequencedInput::new(1, 2, 3, 4, payload);

    let data = rkyv::to_bytes::<_, 256>(&input).expect("failed to serialize");

    println!("data is: {:?}", data);

    // Start a server
    let server_addr = SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, 9999, 0, 0);
    let inbound_server = InboundServer::new(server_addr.port());
    // Set up our decoded message channel.
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    // Initialize the sequencer's polling loop.
    let _join_handle = tokio::spawn(
        inbound_server.start( move |message| {
            let deserialized = message.deserialize(&mut rkyv::Infallible).unwrap(); 
            let res = sender.send( deserialized );
            if let Err(e) = res { 
                panic!("Failed to send on an unbounded channel: {0:?}", e);
            }
        } )
    );

    // Start a client
    let client_addr = SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, 0, 0, 0);
    let client = tokio::net::UdpSocket::bind(&client_addr).await.unwrap();
    println!("client is at {:?}", client.local_addr());
    // client.connect(&server_addr).await.unwrap();

    // Send the archive
    let res = client.send_to(&data, &server_addr).await;
    println!("{:?}", res);

    // Extract a message back out
    let deserialized: UnsequencedInput = receiver.recv().await.unwrap();

    // Make sure we only get ONE message back out.
    timeout(Duration::from_secs(4), receiver.recv()).await.expect_err("Only one message should be sent in this test.");
    
    // Validate the way the sequencer parsed the message.
    assert_eq!(deserialized.app_id, input.app_id);
    assert_eq!(deserialized.instance_id, input.instance_id);
    assert_eq!(deserialized.cluster_id, input.cluster_id);
    assert_eq!(deserialized.sequence_number, input.sequence_number);

    let deserialized_message = String::from_utf8_lossy(&deserialized.payload); 

    assert_eq!(example_message, deserialized_message);
}