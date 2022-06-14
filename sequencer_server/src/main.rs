use bytecheck::StructCheckError;
use clap::Parser;
use rkyv::{validation::{validators::DefaultValidatorError, CheckArchiveError}, Archive};
use sequencer_common::{AppId, ArchivedSequencerMessage, SequencerMessage};
use std::{
    collections::HashMap,
    fmt::Display,
    io::ErrorKind as IoErrorKind,
    net::{Ipv6Addr, SocketAddrV6},
    pin::Pin,
};
use tokio::{self, net::UdpSocket};

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
    /// The sequencer could not bind the local socket required to read incoming
    /// UDP packets.
    UdpBindError(u16, std::io::Error),
    /// socket.recv_from() failed
    UdpReceiveError(std::io::Error),
    /// A message was received for which the SequencerMessage header was invalid
    /// and could not be read by rkyv. We have to first format the
    /// underlying error to a string because a CheckArchiveError is not Send
    CouldNotParse(String),
    /// Duplicate message received. Non-fatal error, but useful to propagate for metrics.
    DuplicateMessage(AppId, u64, u64)
}

impl Display for SequencerReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SequencerReadError::UdpBindError(port, source_error) => write!(
                f,
                "Could not bind a UDP socket on port {}: {:?}",
                port, source_error
            ),
            SequencerReadError::UdpReceiveError(source_error) => write!(
                f,
                "IO error while reading from UDP socket: {:?}",
                source_error
            ),
            SequencerReadError::CouldNotParse(source_error) => write!(
                f,
                "Unable to parse an incoming SequencerMessage header: {:?}",
                source_error
            ),
            SequencerReadError::DuplicateMessage(app_id, sequence, highest) => write!(
                f,
                "Duplicate message received from app {}, message sequence number: {}. Previous highest sequence number encountered from this app is {}",
                app_id,
                sequence,
                highest
            ),
        }
    }
}

impl std::error::Error for SequencerReadError {}

pub enum SequencerReadErrorKind {
    /// The sequencer could not bind the local socket required to read incoming
    /// UDP packets.
    UdpBindError,
    /// socket.recv_from() failed
    UdpReceiveError,
    /// A message was received for which the SequencerMessage header was invalid
    /// and could not be read by rkyv. We have to first format the
    /// underlying error to a string because a CheckArchiveError is not Send
    CouldNotParse,
    /// Duplicate message received. Non-fatal error, but useful to propagate for metrics.
    DuplicateMessage
}

impl SequencerReadError {
    pub fn get_kind(&self) -> SequencerReadErrorKind { 
        match self {
            SequencerReadError::UdpBindError(_, _) => SequencerReadErrorKind::UdpBindError,
            SequencerReadError::UdpReceiveError(_) => SequencerReadErrorKind::UdpReceiveError,
            SequencerReadError::CouldNotParse(_) => SequencerReadErrorKind::CouldNotParse,
            SequencerReadError::DuplicateMessage(_, _, _) => SequencerReadErrorKind::DuplicateMessage,
        }
    }
}

/// Parses inbound messages.
/// Deduplicates and stamps messages with current app-wide sequence number.
struct InboundServer {
    port: u16,
    /// Global, always-monotonically-increasing counter tracking the canonical
    /// order of events, always holds the sequence number of the previous
    /// message sent out over the bus.
    pub counter: u64,
    /// Total bytes written this epoch. Used for "offset"
    pub _total_written: u64,
    /// Last-received sequence numbers per-app.
    /// Used to deduplicate messages.
    app_sequence_numbers: HashMap<AppId, u64>,
}

impl InboundServer {
    pub fn new(port: u16) -> InboundServer {
        InboundServer {
            port,
            counter: 0,
            _total_written: 0,
            app_sequence_numbers: HashMap::default(),
        }
    }

    #[inline(always)]
    pub fn update_counter<'a>(
        &mut self,
        message: Pin<&'a mut ArchivedSequencerMessage>,
    ) -> Pin<&'a mut ArchivedSequencerMessage> {
        self.counter += 1;

        message.modify_sequence_number(self.counter)
    }
    
    #[inline]
    pub async fn process_inbound_message<'a>(&mut self, data: Pin<&'a mut <SequencerMessage as Archive>::Archived>) -> std::result::Result<Pin<&'a mut ArchivedSequencerMessage>, SequencerReadError> { 
        let app_id = data.app_id;
        let app_sequence_number = data.app_sequence_number;

        if let Some(previous_app_seq) = self.app_sequence_numbers.get_mut(&app_id) {
            if *previous_app_seq >= app_sequence_number {
                //We have seen this before, it's old news.
                // Deduplicate.
                Err(SequencerReadError::DuplicateMessage(app_id, app_sequence_number, *previous_app_seq))
            } else {
                //New sequence number! Publish this message.
                *previous_app_seq = app_sequence_number;

                let data = self.update_counter(data);
                Ok(data)
            }
        } else {
            // We have not seen this app ID yet, add it to the dict.
            self.app_sequence_numbers
                .insert(app_id, app_sequence_number);
            let data = self.update_counter(data);
            Ok(data)
        }
    }

    #[inline]
    pub async fn poll_message<'a>(&mut self, socket: &UdpSocket, recv_buffer: &'a mut Vec<u8>) -> Result<Pin<&'a mut ArchivedSequencerMessage>, SequencerReadError> { 
        let (len, _addr) = socket
            .recv_from(recv_buffer)
            .await
            .map_err(SequencerReadError::UdpReceiveError)?;
        rkyv::check_archived_root::<SequencerMessage>(&recv_buffer[..len])
            .map_err(|e| 
                SequencerReadError::CouldNotParse(format!("{:?}", e))
            )?;
        // Per discussion with the rkyv author on Discord, calling check_archived_root()
        // and then only calling archived_root_mut() if the
        // validation succeeds should be very safe, assuming
        // no critical bugs present in check_archived_root()
        // The Rkyv author also mentioned that no work should get duplicated here.
        let data = unsafe {
            rkyv::archived_root_mut::<SequencerMessage>(Pin::new(
                &mut recv_buffer[..len],
            ))
        };
        Ok(data)
    }

    pub async fn step(&mut self, socket: &UdpSocket, recv_buffer: &mut Vec<u8>) -> std::result::Result<(), SequencerReadError> {
        let data = self.poll_message(socket, recv_buffer).await?;
        let message = match self.process_inbound_message(data).await {
            Ok(msg) => Some(msg),
            Err(e) => match e.get_kind() {
                SequencerReadErrorKind::DuplicateMessage => { 
                    // Only in debug builds - avoid formatting strings in the hot path in release. 
                    #[cfg(debug_assertions)]
                    {
                        println!("Encountered duplicate message: {:?}", e);
                    }
                    // Duplicate message, ignore. 
                    None
                },
                _ => {
                    // Irrecoverable error 
                    return Err(e);
                }
            },
        };
        //todo: use message for something. 
        if let Some(_msg) = message {
            
        }
        Ok(())
    }

    pub async fn start(mut self) -> std::result::Result<(), SequencerReadError> {
        let addr = SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, self.port, 0, 0);
        let listener = UdpSocket::bind(addr)
            .await
            .map_err(|e| SequencerReadError::CouldNotParse(format!("{:?}", e)))?;

        println!("Hello, world on port {:?}!", listener);
        let mut recv_buffer = vec![0u8; 2048];
        loop {
            self.step( &listener, &mut recv_buffer).await?;
        }
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> std::io::Result<()> {
    let args = Args::parse();

    let inbound_server = InboundServer::new(args.port);
    let join_handle =
        tokio::spawn(inbound_server.start());

    join_handle
        .await?
        .map_err(|e| std::io::Error::new(IoErrorKind::InvalidData, Box::new(e)))
}


// Tests

#[cfg(test)]
mod test {
    use super::*;
    use lazy_static::lazy_static;
    use tokio::sync::Mutex;

    // Any localhost-related network tests will interfere with eachother if you use
    // the cargo test command, which is multithreaded by default.
    // Passing -- --test-threads=1 will get around this, but this may be unclear to
    // new users (and, possibly, to CI servers!) so I have created this mutex as
    // a workaround.
    lazy_static! {
        static ref IO_TEST_PERMISSIONS: Mutex<()> = Mutex::new(());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_stream_received() {
        use rkyv::Deserialize;

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
        let mut inbound_server = InboundServer::new(server_addr.port());

        let server_listener = UdpSocket::bind(server_addr)
            .await
            .map_err(|e| SequencerReadError::CouldNotParse(format!("{:?}", e))).unwrap();

        // Start a client
        let client_addr = SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, 0, 0, 0);
        let client = tokio::net::UdpSocket::bind(&client_addr).await.unwrap();
        println!("client is at {:?}", client.local_addr());
        // client.connect(&server_addr).await.unwrap();

        // Send the archive
        let res = client.send_to(&data, &server_addr).await;
        println!("{:?}", res);


        let mut recv_buffer = vec![0u8; 2048];
        let raw_message: Pin<&mut ArchivedSequencerMessage> = inbound_server.poll_message(&server_listener, &mut recv_buffer).await.unwrap(); 
        let message: Pin<&mut ArchivedSequencerMessage> = inbound_server.process_inbound_message(raw_message).await.unwrap();
        // Extract a message back out
        // This always returns With<_, _>, no matter 
        // how I try to finagle it. So, commenting this test out for now. 
        let deserialized: SequencerMessage = message.as_ref().get_ref().deserialize(&mut rkyv::Infallible::default()).unwrap();

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
        use chrono::prelude::*;

        let mut total_received: usize = 0;

        // Prevent tests from interfering with eachother over the localhost connection.
        // This should implicitly drop when the test ends.
        let _guard = IO_TEST_PERMISSIONS.lock().await;

        // Start a server
        let server_addr = SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, 9999, 0, 0);
        let mut inbound_server = InboundServer::new(server_addr.port());

        let server_listener = UdpSocket::bind(server_addr)
            .await
            .map_err(|e| SequencerReadError::CouldNotParse(format!("{:?}", e))).unwrap();

        // Send some messages.
        const NUM_TEST_SENDERS: usize = 20;
        const NUM_MESSAGE_TEST: usize = 10;

        println!("Sending messages which should not be deduplicated (each unique).");

        for app_id in 0..NUM_TEST_SENDERS {
            for i in 0..NUM_MESSAGE_TEST {
                // Dummy message for testing.
                let example_message = format!("Hello, world, number {}!", i);
                let payload = example_message.as_bytes().to_vec();
                // Build an archive
                let input = SequencerMessage::new(app_id as AppId, 1, 2, i as u64, payload);
                let data = rkyv::to_bytes::<_, 256>(&input).expect("failed to serialize");

                // Start a client
                let client_addr = SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, 0, 0, 0);
                let client = tokio::net::UdpSocket::bind(&client_addr).await.unwrap();
                // client.connect(&server_addr).await.unwrap();

                // Send the archive
                client
                    .send_to(&data, &server_addr)
                    .await
                    .expect("Failed to send");
            }
        }

        let mut recv_buffer = vec![0u8; 2048];
        for _i in 0 .. (NUM_TEST_SENDERS*NUM_MESSAGE_TEST) as u64 { 
            total_received += 1;
            println!("[{:?}] Processing message # {}", Utc::now(), total_received);

            let prev_counter = inbound_server.counter;
            let data = inbound_server.poll_message( &server_listener, &mut recv_buffer).await.unwrap();
            //There should be no duplicates here.
            let resl = inbound_server.process_inbound_message(data).await.unwrap();
            assert!(inbound_server.counter > prev_counter);
            assert_eq!(inbound_server.counter, resl.as_ref().sequence_number);
            println!("[{:?}] Counter is now {}", Utc::now(), inbound_server.counter);
        }

        assert_eq!(
            inbound_server.counter,
            (NUM_TEST_SENDERS * NUM_MESSAGE_TEST) as u64
        );

        
        // Now send it some garbage. 
        // It will have seen all of these app-sequence-numbers before
        println!("Sending messages which should be deduplicated (using previous app sequence numbers).");
        for app_id in 0..NUM_TEST_SENDERS {
            for i in 0..NUM_MESSAGE_TEST {
                // Dummy message for testing.
                let example_message = format!("Hello, world, number {}!", i);
                let payload = example_message.as_bytes().to_vec();
                // Build an archive
                let input = SequencerMessage::new(app_id as AppId, 1, 2, i as u64, payload);
                let data = rkyv::to_bytes::<_, 256>(&input).expect("failed to serialize");

                // Start a client
                let client_addr = SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, 0, 0, 0);
                let client = tokio::net::UdpSocket::bind(&client_addr).await.unwrap();
                // client.connect(&server_addr).await.unwrap();

                // Send the archive
                client
                    .send_to(&data, &server_addr)
                    .await
                    .expect("Failed to send");
            }
        }
        for _i in 0..(NUM_TEST_SENDERS*NUM_MESSAGE_TEST) as u64 { 
            total_received += 1;
            println!("[{:?}] Processing message # {}", Utc::now(), total_received);

            let prev_counter = inbound_server.counter;
            let data = inbound_server.poll_message( &server_listener, &mut recv_buffer).await.unwrap();
            //Should always be a duplicate in this context.
            let _resl = inbound_server.process_inbound_message(data).await.unwrap_err();
            assert_eq!(inbound_server.counter, prev_counter);
            println!("[{:?}] Counter is now {}", Utc::now(), inbound_server.counter);
        }

        //Since it has seen all of these app sequence numbers before, the counter
        // should not have changed.
        assert_eq!(
            inbound_server.counter,
            (NUM_TEST_SENDERS * NUM_MESSAGE_TEST) as u64
        );

        //But if we send it a new one, it should recognize it.
        println!("Lastly, sending a new message which should be new and not deduplicated.");
        let example_message = "Foo, and also bar!";
        let payload = example_message.as_bytes().to_vec();
        // Build an archive
        let input = SequencerMessage::new(1_u16, 1, 2, (NUM_MESSAGE_TEST + 1) as u64, payload);
        let data = rkyv::to_bytes::<_, 256>(&input).expect("failed to serialize");

        // Start a client
        let client_addr = SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, 0, 0, 0);
        let client = tokio::net::UdpSocket::bind(&client_addr).await.unwrap();
        // client.connect(&server_addr).await.unwrap();

        // Send the archive
        let res = client.send_to(&data, &server_addr).await;
        res.expect("Failed to send");
        
        total_received += 1;
        println!("[{:?}] Processing message # {}", Utc::now(), total_received);
        let data = inbound_server.poll_message( &server_listener, &mut recv_buffer).await.unwrap();
        let _resl = inbound_server.process_inbound_message(data).await.unwrap();
        println!("[{:?}] Finally, counter is {}", Utc::now(), inbound_server.counter);

        //One new valid, non-duplicate message.
        assert_eq!(
            inbound_server.counter,
            (NUM_TEST_SENDERS * NUM_MESSAGE_TEST) as u64 + 1
        );
    }
}