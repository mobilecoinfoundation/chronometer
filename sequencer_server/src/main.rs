use bytecheck::StructCheckError;
use clap::Parser;
use rkyv::validation::{CheckArchiveError, validators::DefaultValidatorError};
use std::{net::{Ipv6Addr, SocketAddrV6}, fmt::Display, collections::HashMap, pin::Pin};
use std::io::ErrorKind as IoErrorKind;
use tokio::{self, net::UdpSocket};
use sequencer_common::{SequencerMessage, ArchivedSequencerMessage, AppId};

pub mod record;

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
    /// A message was received for which the SequencerMessage header was invalid and could not be read by rkyv.
    /// We have to first format the underlying error to a string because a CheckArchiveError is not Send
    CouldNotParse(String),
}

impl Display for SequencerReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SequencerReadError::UdpBindError(port, source_error) => write!(f, "Could not bind a UDP socket on port {}: {:?}", port, source_error),
            SequencerReadError::UdpReceiveError(source_error) => write!(f, "IO error while reading from UDP socket: {:?}", source_error),
            SequencerReadError::CouldNotParse(source_error) => write!(f, "Unable to parse an incoming SequencerMessage header: {:?}", source_error),
        }
    }
}

impl std::error::Error for SequencerReadError {}

impl From<ReadArchiveError> for SequencerReadError {
    fn from(e: ReadArchiveError) -> Self {
        Self::CouldNotParse( format!("{:?}", e) )
    }
}

/// Parses inbound messages. 
/// Deduplicates and stamps messages with current app-wide sequence number.
struct InboundServer {
    port: u16,
    /// Global, always-monotonically-increasing counter tracking the canonical order of events, always 
    /// holds the sequence number of the previous message sent out over the bus.
    pub counter: u64,
    /// Total bytes written this epoch. Used for "offset"
    pub _total_written: u64,
    /// Last-received sequence numbers per-app. 
    /// Used to deduplicate messages.
    app_sequence_numbers: HashMap<AppId, u64>,
}

impl InboundServer {
    pub fn new(port: u16) -> InboundServer {
        InboundServer { port, counter: 0, _total_written: 0, app_sequence_numbers: HashMap::default() }
    }

    #[inline(always)]
    pub fn update_counter<'a>(&mut self, message: Pin<&'a mut ArchivedSequencerMessage>) -> Pin<&'a mut ArchivedSequencerMessage> {
        self.counter += 1;

        message.with_sequence_number(self.counter)
    }

    pub async fn start<F>(mut self, closure: F) -> std::result::Result<(), SequencerReadError>
        where F: Fn(Pin<&ArchivedSequencerMessage>) {
        let addr = SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, self.port, 0, 0);
        let listener = UdpSocket::bind(addr).await
            .map_err(|e| SequencerReadError::CouldNotParse(format!("{:?}", e)))?;

        println!("Hello, world on port {:?}!", listener);
        let mut recv_buffer = vec![0u8; 2048];
        loop {
            let (len, addr) = listener.recv_from(&mut recv_buffer).await
                .map_err(SequencerReadError::UdpReceiveError)?;
            println!("{}, {:?}", len, addr);
            let data_res = rkyv::check_archived_root::<SequencerMessage>(&recv_buffer[..len]).map(|_| ());
            

            match data_res {
                Ok(()) => {
                    // Per discussion with the rkyv author on Discord, calling check_archived_root() and then 
                    // only calling archived_root_mut() if the validation succeeds should be very safe, assuming
                    // no critical bugs present in check_archived_root()
                    // The Rkyv author also mentioned that no work should get duplicated here.
                    let data  = unsafe { rkyv::archived_root_mut::<SequencerMessage>(Pin::new(&mut recv_buffer[..len])) };

                    let app_id = data.app_id;
                    let app_sequence_number = data.app_sequence_number; 

                    if let Some(previous_app_seq) = self.app_sequence_numbers.get_mut(&app_id) { 
                        if *previous_app_seq >= app_sequence_number { 
                            //We have seen this before, it's old news. Deduplicate.
                        }
                        else { 
                            //New sequence number! Publish this message.
                            *previous_app_seq = app_sequence_number;

                            let data = self.update_counter(data);
                            closure(data.into_ref());
                        }
                    }
                    else {
                        // We have not seen this app ID yet, add it to the dict. 
                        self.app_sequence_numbers.insert(app_id, app_sequence_number);
                        let data = self.update_counter(data);
                        closure(data.into_ref());
                    }
                }
                Err(e) => {
                    return Err(e.into());
                }
            }
        }
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> std::io::Result<()> {
    let args = Args::parse();

    let inbound_server = InboundServer::new(args.port);
    let join_handle = tokio::spawn(inbound_server.start(|message| println!("Received: {:?}", message) ));

    join_handle.await?.map_err(|e| std::io::Error::new(IoErrorKind::InvalidData, Box::new(e)) )
}


#[cfg(test)]
mod test {
    use tokio::sync::Mutex;
    use lazy_static::lazy_static;
    use super::*;

    // Any localhost-related network tests will interfere with eachother if you use the 
    // cargo test command, which is multithreaded by default.
    // Passing -- --test-threads=1 will get around this, but this may be unclear to new users (and, possibly, to CI servers!)
    // so I have created this mutex as a workaround. 
    lazy_static!{ 
        static ref IO_TEST_PERMISSIONS: Mutex<()> = Mutex::new(());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_stream_received() {
        use rkyv::Deserialize;
        use std::time::Duration;
        use tokio::time::timeout;

        // Prevent tests from interfering with eachother over the localhost connection.
        // This should implicitly drop when the test ends.
        let _guard = IO_TEST_PERMISSIONS.lock().await;

        // Dummy message for testing. 
        let example_message = "Hello, world!";
        let payload = example_message.as_bytes().to_vec();
        // Build an archive
        let input = SequencerMessage::new(1, 2, 3, 4, payload);

        let data = rkyv::to_bytes::<_, 256>(&input).expect("failed to serialize");

        println!("data is: {:?}", data);

        // Start a server
        let server_addr = SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, 9999, 0, 0);
        let inbound_server = InboundServer::new(server_addr.port());
        // Set up our decoded message channel.
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        // Initialize the sequencer's polling loop.
        let _join_handle = tokio::spawn(
            inbound_server.start( 
            #[inline(always)]
                move |message| {
                    let deserialized: SequencerMessage = message.get_ref().deserialize(&mut rkyv::Infallible).unwrap(); 
                    let res = sender.send( deserialized );
                    if let Err(e) = res { 
                        panic!("Failed to send on an unbounded channel: {0:?}", e);
                    }
                } 
            )
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
        let deserialized = receiver.recv().await.unwrap();

        // Make sure we only get ONE message back out.
        timeout(Duration::from_secs(4), receiver.recv()).await.expect_err("Only one message should be sent in this test.");
        
        // Validate the way the sequencer parsed the message.
        assert_eq!(deserialized.app_id, input.app_id);
        assert_eq!(deserialized.instance_id, input.instance_id);
        assert_eq!(deserialized.cluster_id, input.cluster_id);
        assert_eq!(deserialized.app_sequence_number, input.app_sequence_number);

        let deserialized_message = String::from_utf8_lossy(&deserialized.payload); 

        assert_eq!(example_message, deserialized_message);
    }
    
    #[tokio::test(flavor = "multi_thread")]
    async fn test_counter() {
        use rkyv::Deserialize;
        use std::time::Duration;
        use std::sync::atomic::{Ordering, AtomicU64};
        use std::sync::Arc;

        // Prevent tests from interfering with eachother over the localhost connection.
        // This should implicitly drop when the test ends.
        let _guard = IO_TEST_PERMISSIONS.lock().await;

        // Start a server
        let server_addr = SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, 9999, 0, 0);
        let inbound_server = InboundServer::new(server_addr.port());


        let last_counter = Arc::new(AtomicU64::new(0) );
        let last_counter_ref = last_counter.clone();

        // Initialize the sequencer's polling loop.
        let _server_handle = tokio::spawn(
            inbound_server.start( 
            #[inline(always)]
                move |message| {
                    let deserialized: SequencerMessage = message.get_ref().deserialize(&mut rkyv::Infallible).unwrap();
                    //This should be after the sequencer put a counter on it. 
                    let counter = deserialized.sequence_number;
                    assert!(counter > last_counter_ref.load(Ordering::Relaxed));
                    last_counter_ref.store(counter, Ordering::Relaxed)
                }
            )
        );

        // Send some messages.
        const NUM_TEST_SENDERS: usize = 20; 
        const NUM_MESSAGE_TEST: usize = 10; 
        
        for app_id in 0..NUM_TEST_SENDERS { 
            for i in 0..NUM_MESSAGE_TEST {
                // Dummy message for testing. 
                let example_message = format!("Hello, world, number {}!", i);
                let payload = example_message.as_bytes().to_vec();
                // Build an archive
                let input = SequencerMessage::new(app_id as u32, 1, 2, i as u64, payload);
                let data = rkyv::to_bytes::<_, 256>(&input).expect("failed to serialize");

                // Start a client
                let client_addr = SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, 0, 0, 0);
                let client = tokio::net::UdpSocket::bind(&client_addr).await.unwrap();
                // client.connect(&server_addr).await.unwrap();

                // Send the archive
                client.send_to(&data, &server_addr).await.expect("Failed to send");
            }
        }
        //Give the server a moment to catch up
        tokio::time::sleep(Duration::from_millis(50)).await;
        //Let's see if the counter is valid...
        assert_eq!(last_counter.load(Ordering::Relaxed), (NUM_TEST_SENDERS*NUM_MESSAGE_TEST) as u64);


        // Now send it some garbage. It will have seen all of these app-sequence-numbers before
        for app_id in 0..NUM_TEST_SENDERS { 
            for i in 0..NUM_MESSAGE_TEST {
                // Dummy message for testing. 
                let example_message = format!("Hello, world, number {}!", i);
                let payload = example_message.as_bytes().to_vec();
                // Build an archive
                let input = SequencerMessage::new(app_id as u32, 1, 2, i as u64, payload);
                let data = rkyv::to_bytes::<_, 256>(&input).expect("failed to serialize");

                // Start a client
                let client_addr = SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, 0, 0, 0);
                let client = tokio::net::UdpSocket::bind(&client_addr).await.unwrap();
                // client.connect(&server_addr).await.unwrap();

                // Send the archive
                client.send_to(&data, &server_addr).await.expect("Failed to send");
            }
        }
        //Give the server a moment to catch up
        tokio::time::sleep(Duration::from_millis(50)).await;
        //Since it has seen all of these app sequence numbers before, the counter should not have changed.
        assert_eq!(last_counter.load(Ordering::Relaxed), (NUM_TEST_SENDERS*NUM_MESSAGE_TEST) as u64);

        //But if we send it a new one, it should recognize it.
        let example_message = "Foo, and also bar!";
        let payload = example_message.as_bytes().to_vec();
        // Build an archive
        let input = SequencerMessage::new(1u32, 1, 2, (NUM_MESSAGE_TEST+1) as u64, payload);
        let data = rkyv::to_bytes::<_, 256>(&input).expect("failed to serialize");

        // Start a client
        let client_addr = SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, 0, 0, 0);
        let client = tokio::net::UdpSocket::bind(&client_addr).await.unwrap();
        // client.connect(&server_addr).await.unwrap();

        // Send the archive
        let res = client.send_to(&data, &server_addr).await;
        res.expect("Failed to send");
        //Give the server a moment to catch up
        tokio::time::sleep(Duration::from_millis(50)).await;
        //One new valid, non-duplicate message.
        assert_eq!(last_counter.load(Ordering::Relaxed), (NUM_TEST_SENDERS*NUM_MESSAGE_TEST) as u64 + 1);
    }
}