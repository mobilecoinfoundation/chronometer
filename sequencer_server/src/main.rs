use bytecheck::StructCheckError;
use clap::Parser;
use fxhash::FxHashMap;
use record::MessageRecordBackend;
use rkyv::{
    validation::{validators::DefaultValidatorError, CheckArchiveError},
    Archive,
};
use sequencer_common::{AppId, ArchivedSequencerMessage, SequencerMessage, EpochId};
use tokio::{sync::broadcast::Receiver, io::{AsyncReadExt, AsyncWriteExt}};
use std::{
    collections::HashMap,
    fmt::Display,
    io::{self, ErrorKind as IoErrorKind},
    net::{Ipv6Addr, SocketAddrV6, UdpSocket, IpAddr, SocketAddr},
    path::PathBuf,
    pin::Pin,
    sync::atomic::{AtomicU64, self}, time::Duration, str::FromStr,
};

pub mod record;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(short, long, default_value_t = 1)]
    pub instance: u16,
    #[clap(short, long, default_value_t = 0)]
    pub port: u16,
    #[clap(short, long)]
    pub address: String,
    #[clap(short = 'p', long = "recordpath")]
    pub record_path: PathBuf,
    #[clap(short = 'e', long = "epoch", default_value_t = 0)]
    pub epoch: EpochId,
    #[clap(short = 'o', long = "start_offset", default_value_t = 0)]
    pub offset: u64,
    #[clap(short = 's', long = "start_seq", default_value_t = 0)]
    pub sequence: u64,
}

pub type ReadArchiveError = CheckArchiveError<StructCheckError, DefaultValidatorError>;

/// How many bytes have been published to the message bus (during this epoch) in
/// total?
pub static CURRENT_OFFSET: AtomicU64 = AtomicU64::new(0u64);

#[derive(Debug)]
pub enum SequencerError {
    /// A message was received for which the SequencerMessage header was invalid
    /// and could not be read by rkyv. We have to first format the
    /// underlying error to a string because a CheckArchiveError is not Send
    CouldNotParse(String),
    /// Duplicate message received. Non-fatal error, but useful to propagate for
    /// metrics.
    DuplicateMessage(AppId, u64, u64),
    /// The sequencer could not bind the local socket
    /// required to read incoming UDP packets.
    UdpBindError(u16, std::io::Error),
    /// socket.recv_from() failed
    UdpReceiveError(std::io::Error),
    /// Unable to write a received message to our record file.
    WriteToFile(std::io::Error),
    /// Failed to change the properties of a UDPSocket.
    SocketChangeFailed(std::io::Error),
}

impl Display for SequencerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SequencerError::CouldNotParse(source_error) => write!(
                f,
                "Unable to parse an incoming SequencerMessage header: {:?}",
                source_error
            ),
            SequencerError::DuplicateMessage(app_id, sequence, highest) => write!(
                f,
                "Duplicate message received from app {}, message sequence number: {}. Previous highest sequence number encountered from this app is {}",
                app_id,
                sequence,
                highest
            ),
            SequencerError::UdpBindError(port, source_error) => write!(
                f,
                "Could not bind a UDP socket on port {}: {:?}",
                port, source_error
            ),
            SequencerError::UdpReceiveError(source_error) => write!(
                f,
                "IO error while reading from UDP socket: {:?}",
                source_error
            ),
            SequencerError::WriteToFile(source_error) => write!(
                f,
                "IO error, unable to write message to our record file: {:?}",
                source_error
            ),
            SequencerError::SocketChangeFailed(source_error) => write!(
                f,
                "Could not change the configuration of a UDP socket: {:?}",
                source_error
            ),
        }
    }
}

impl std::error::Error for SequencerError {}

pub enum SequencerErrorKind {
    /// A message was received for which the SequencerMessage header was invalid
    /// and could not be read by rkyv. We have to first format the
    /// underlying error to a string because a CheckArchiveError is not Send
    CouldNotParse,
    /// Duplicate message received. Non-fatal error, but useful to propagate for
    /// metrics.
    DuplicateMessage,
    /// The sequencer could not bind the local socket required to read incoming
    /// UDP packets.
    UdpBindError,
    /// socket.recv_from() failed
    UdpReceiveError,
    /// Unable to write a received message to our record file.
    WriteToFile,
    /// Failed to change the properties of a UDPSocket.
    SocketChangeFailed,
}

impl SequencerError {
    pub fn get_kind(&self) -> SequencerErrorKind {
        match self {
            SequencerError::CouldNotParse(_) => SequencerErrorKind::CouldNotParse,
            SequencerError::DuplicateMessage(_, _, _) => SequencerErrorKind::DuplicateMessage,
            SequencerError::UdpBindError(_, _) => SequencerErrorKind::UdpBindError,
            SequencerError::UdpReceiveError(_) => SequencerErrorKind::UdpReceiveError,
            SequencerError::WriteToFile(_) => SequencerErrorKind::WriteToFile,
            SequencerError::SocketChangeFailed(_) => SequencerErrorKind::SocketChangeFailed,
        }
    }
}

/// Parses inbound messages.
/// Deduplicates and stamps messages with current app-wide sequence number.
struct InboundServer<Record: MessageRecordBackend> {
    port: u16,
    /// Global, always-monotonically-increasing counter tracking the canonical
    /// order of events, always holds the sequence number of the previous
    /// message sent out over the bus.
    pub counter: u64,
    /// Total bytes written this epoch. Used for "offset"
    pub _total_written: u64,
    /// Last-received sequence numbers per-app.
    /// Used to deduplicate messages.
    /// 
    /// This is an FxHashMap because the sequencer will always be running 
    /// inside a VPC, and it shouldn't be ingesting untrusted traffic in 
    /// general - a firewall is needed. So, hash-dos attacks are not a concern.  
    app_sequence_numbers: FxHashMap<AppId, u64>,
    /// Underlying storage to which we write our messages.
    pub record: Record,
}

impl<Record> InboundServer<Record>
where
    Record: MessageRecordBackend,
{
    pub fn new(port: u16, record: Record, starting_offset: u64, starting_seq: u64) -> InboundServer<Record> {
        InboundServer {
            port,
            counter: starting_seq,
            _total_written: starting_offset,
            app_sequence_numbers: HashMap::default(),
            record,
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
    pub fn process_inbound_message<'a>(
        &mut self,
        data: Pin<&'a mut <SequencerMessage as Archive>::Archived>,
    ) -> std::result::Result<Pin<&'a mut ArchivedSequencerMessage>, SequencerError> {
        let app_id = data.app_id;
        let app_sequence_number = data.app_sequence_number;

        if let Some(previous_app_seq) = self.app_sequence_numbers.get_mut(&app_id) {
            if *previous_app_seq >= app_sequence_number {
                //We have seen this before, it's old news.
                // Deduplicate.
                Err(SequencerError::DuplicateMessage(
                    app_id,
                    app_sequence_number,
                    *previous_app_seq,
                ))
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

    /// Write the provided message to our record file.
    #[inline]
    pub fn write_message<T: AsRef<[u8]>>(
        &mut self,
        message: T,
    ) -> std::result::Result<(), SequencerError> {
        self.record
            .write_message(message)
            .map_err(SequencerError::WriteToFile)?;
        let offset = self.record.record_len();
        CURRENT_OFFSET.store(offset, std::sync::atomic::Ordering::Relaxed); // TODO - Should we be using another ordering?
        Ok(())
    }

    #[inline]
    pub fn poll_message<'a>(
        &mut self,
        socket: &UdpSocket,
        recv_buffer: &'a mut Vec<u8>,
    ) -> Result<(Pin<&'a mut ArchivedSequencerMessage>, u64), SequencerError> {
        let (message_len, _addr) = socket
            .recv_from(recv_buffer)
            .map_err(SequencerError::UdpReceiveError)?;
        rkyv::check_archived_root::<SequencerMessage>(&recv_buffer[..message_len])
            .map_err(|e| SequencerError::CouldNotParse(format!("{:?}", e)))?;
        // Per discussion with the rkyv author on Discord, calling check_archived_root()
        // and then only calling archived_root_mut() if the
        // validation succeeds should be very safe, assuming
        // no critical bugs present in check_archived_root()
        // The Rkyv author also mentioned that no work should get duplicated here.
        let data = unsafe {
            rkyv::archived_root_mut::<SequencerMessage>(Pin::new(&mut recv_buffer[..message_len]))
        };
        Ok((data, message_len as u64))
    }

    #[inline]
    pub fn step(
        &mut self,
        socket: &UdpSocket,
        recv_buffer: &mut Vec<u8>,
    ) -> std::result::Result<(), SequencerError> {
        let (data, message_len) = self.poll_message(socket, recv_buffer)?;
        let message_maybe = match self.process_inbound_message(data) {
            Ok(msg) => Some(msg),
            Err(e) => match e.get_kind() {
                SequencerErrorKind::DuplicateMessage => {
                    // Only in debug builds - avoid formatting strings in the hot path in release.
                    #[cfg(debug_assertions)]
                    {
                        println!("Encountered duplicate message: {:?}", e);
                    }
                    // Duplicate message, ignore.
                    None
                }
                _ => {
                    // Irrecoverable error
                    return Err(e);
                }
            },
        };
        if message_maybe.is_some() {
            drop(message_maybe);
            if message_len > 0 {
                self.write_message(&recv_buffer[0..message_len as usize])?;
            }
        }
        Ok(())
    }

    pub fn start(mut self) -> std::result::Result<(), SequencerError> {
        let addr = SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, self.port, 0, 0);
        let listener = UdpSocket::bind(&addr)
            .map_err(|e| SequencerError::CouldNotParse(format!("{:?}", e)))?;
        listener
            .set_nonblocking(false)
            .map_err(SequencerError::SocketChangeFailed)?;

        println!("Launching server, bound to socket {:?}", &addr);
        let mut recv_buffer = vec![0u8; 2048];
        loop {
            self.step(&listener, &mut recv_buffer)?;
        }
    }

    #[allow(dead_code)]
    /// Consumes the server object, returning an owned copy of
    /// the MessageRecordBackend written to. Used mostly for testing.
    pub fn into_record(self) -> Record {
        self.record
    }
}

/// How many worker threads can we safely spin up
/// without fighting with the sequencer thread for cycles?
/// Returns (non_blocking, blocking)
fn estimate_threads_for_async() -> (usize, usize) {
    // Number of threads reserved for the sequencer
    // (and also possibly the OS depending on what kinds of guesses we want to make
    // here)
    // Currently it's the sequencer, the tokio blocking thread and the counter watcher thread. 
    const RESERVED: usize = 3;

    let available_threads: usize = match std::thread::available_parallelism() {
        Ok(val) => val.into(),
        Err(e) => {
            eprintln!("Unable to get a count of available threads from the OS, defaulting to 1. Error was: {:?}", e);
            eprintln!("Unable to get a count of available threads from the OS, defaulting to 1. Error was: {:?}", e);
            1
        }
    };

    let non_blocking = if (available_threads as i64) - (RESERVED as i64) < 1
    {
        1usize
    } else {
        ((available_threads as i64) - (RESERVED as i64)) as usize
    };

    (non_blocking, 1)
}

fn build_async_runtime() -> io::Result<tokio::runtime::Runtime> {
    let (non_blocking_threads, blocking_threads) = estimate_threads_for_async();
    println!("Initializing Tokio runtime with {} async worker threads and {} blocking threads provided to it.", non_blocking_threads, blocking_threads);
    let mut runtime_builder = tokio::runtime::Builder::new_multi_thread();
    runtime_builder.enable_all();
    runtime_builder.max_blocking_threads(blocking_threads);
    runtime_builder.worker_threads(non_blocking_threads);

    runtime_builder.build()
}

fn offset_watcher_thread(channel_capacity: usize, update_interval: Duration) -> Receiver<u64> { 
    // Intended to notify all Client-Service processes, which will likely be 
    let (sender, receiver) = tokio::sync::broadcast::channel(channel_capacity);
    std::thread::spawn(move || {
        let mut previous_offset: u64 = CURRENT_OFFSET.load(atomic::Ordering::Relaxed); 
        loop { 
            std::thread::sleep(update_interval);
            let new_offset: u64 = CURRENT_OFFSET.load(atomic::Ordering::Relaxed); 
            if new_offset > previous_offset { 
                sender.send(new_offset-previous_offset).unwrap();
                previous_offset = new_offset;
            }
        }
    });
    receiver
}

fn main() -> std::io::Result<()> {
    let args = Args::parse();

    #[cfg(debug_assertions)]
    {
        println!("This is a debug build - please build the sequencer in release mode for production use.")
    }

    //Make sure the path exists.
    let mut path: PathBuf = args.record_path;
    //No record file?
    if path.as_os_str().is_empty() {
        // TODO: Figure out better defaults for record file path.
        path = PathBuf::from("messagebus");
    }
    //Make sure our directory exists.
    if let Some(parent) = path.parent() {
        if parent.is_dir() {
            std::fs::create_dir_all(parent)?;
        }
    }
    
    let runtime = build_async_runtime().unwrap();

    // Load our record file. 
    let record = record::MemMapBackend::init(path, args.offset)?;
    // Determine current offset. 
    let initial_offset = record.initial_offset();
    CURRENT_OFFSET.store(initial_offset, atomic::Ordering::Relaxed); 

    let offset_change_receiver = offset_watcher_thread(4096, Duration::from_millis(50));

    let tcp_addr = args.address.clone();
    let tcp_port = args.port; 

    runtime.spawn(async move {
        let address = match IpAddr::from_str(&tcp_addr) {
            Ok(addr) => SocketAddr::from((addr, tcp_port)),
            Err(parse_err) => match tokio::net::lookup_host(&tcp_addr).await { 
                Ok(mut addrs) => { 
                    SocketAddr::from((addrs.next().unwrap().ip(), tcp_port))
                }, 
                Err(lookup_err) => { 
                    panic!("Failed to parse {} as an IP address - when assuming it was a URL, DNS lookup failed with, {:?}", &tcp_addr, lookup_err) 
                }
            },
        };

        let listener = tokio::net::TcpListener::bind(address).await.unwrap();
        //Make sure the closure captures it before borrowing it off into another closure.
        let offset_change_receiver = offset_change_receiver;
        loop {
            let (mut socket, _peer_addr) = listener.accept().await.unwrap();
            let mut local_change_receiver = offset_change_receiver.resubscribe();
            tokio::spawn(async move {
                let mut read_buf = [0u8; 4096];
                let (mut reader, mut writer) = socket.split();
                loop { 
                    tokio::select! {
                        new_bytes = local_change_receiver.recv() => { 
                            //Todo: Read from mem-mapped file, send off new messages to subscribers.
                        }
                        len_read = reader.read(&mut read_buf) => { 
                            //Todo: Read inbound messages
                        }
                    }
                }
            });
        }
    });

    let inbound_server = InboundServer::new(args.port, record, initial_offset, args.sequence);
    inbound_server
        .start()
        .map_err(|e| std::io::Error::new(IoErrorKind::InvalidData, Box::new(e)))
}

// Tests
#[cfg(test)]
mod test {
    use std::{
        io::Read,
        path::Path,
        sync::{atomic, Mutex}, time::Duration,
    };

    use crate::record::{
        test_util::{DummyBackend, VecBackend},
        MemMapBackend,
    };

    use super::*;
    use lazy_static::lazy_static;
    use tokio::sync::broadcast::{Receiver, Sender};

    // Any localhost-related network tests will interfere with eachother if you use
    // the cargo test command, which is multithreaded by default.
    // Passing -- --test-threads=1 will get around this, but this may be unclear to
    // new users (and, possibly, to CI servers!) so I have created this mutex as
    // a workaround.
    lazy_static! {
        static ref IO_TEST_PERMISSIONS: Mutex<()> = Mutex::new(());
    }

    #[test]
    fn test_stream_received() {
        use rkyv::Deserialize;

        // Prevent tests from interfering with eachother over the localhost connection.
        // This should implicitly drop when the test ends.
        let _guard = IO_TEST_PERMISSIONS.lock();
        CURRENT_OFFSET.store(0, atomic::Ordering::Relaxed);

        // Dummy message for testing.
        let example_message = "Hello, world!";
        let payload = example_message.as_bytes().to_vec();
        // Build an archive
        let input = SequencerMessage::new(1, 2, 3, 4, payload);

        let data = rkyv::to_bytes::<_, 256>(&input).expect("failed to serialize");

        println!("data is: {:?}", data);

        // Start a server
        let server_addr = SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, 9999, 0, 0);
        let mut inbound_server = InboundServer::new(server_addr.port(), DummyBackend {}, 0, 0);

        let server_listener = UdpSocket::bind(server_addr)
            .map_err(|e| SequencerError::CouldNotParse(format!("{:?}", e)))
            .unwrap();

        // Start a client
        let client_addr = SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, 0, 0, 0);
        let client = UdpSocket::bind(&client_addr).unwrap();
        println!("client is at {:?}", client.local_addr());
        // client.connect(&server_addr).unwrap();

        // Send the archive
        let res = client.send_to(&data, &server_addr);
        println!("{:?}", res);

        let mut recv_buffer = vec![0u8; 2048];
        let (raw_message, _len): (Pin<&mut ArchivedSequencerMessage>, u64) = inbound_server
            .poll_message(&server_listener, &mut recv_buffer)
            .unwrap();
        let message: Pin<&mut ArchivedSequencerMessage> =
            inbound_server.process_inbound_message(raw_message).unwrap();
        // Extract a message back out
        // This always returns With<_, _>, no matter
        // how I try to finagle it. So, commenting this test out for now.
        let deserialized: SequencerMessage = message
            .as_ref()
            .get_ref()
            .deserialize(&mut rkyv::Infallible::default())
            .unwrap();

        // Validate the way the sequencer parsed the message.
        assert_eq!(deserialized.app_id, input.app_id);
        assert_eq!(deserialized.instance_id, input.instance_id);
        assert_eq!(deserialized.cluster_id, input.cluster_id);
        assert_eq!(deserialized.app_sequence_number, input.app_sequence_number);

        let deserialized_message = String::from_utf8_lossy(&deserialized.payload);

        assert_eq!(example_message, deserialized_message);
    }

    #[test]
    fn test_counter() {
        // Prevent tests from interfering with eachother over the localhost connection.
        // This should implicitly drop when the test ends.
        let _guard = IO_TEST_PERMISSIONS.lock();
        
        // Start a server
        let server_addr = SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, 9999, 0, 0);
        let mut inbound_server = InboundServer::new(server_addr.port(), DummyBackend {}, 0, 0);

        let server_listener = UdpSocket::bind(server_addr)
            .map_err(|e| SequencerError::CouldNotParse(format!("{:?}", e)))
            .unwrap();

        // Send some messages.
        const NUM_TEST_SENDERS: usize = 20;
        const NUM_MESSAGE_TEST: usize = 10;

        //println!("Sending messages which should not be deduplicated (each unique).");

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
                let client = UdpSocket::bind(&client_addr).unwrap();
                // client.connect(&server_addr).unwrap();

                // Send the archive
                client.send_to(&data, &server_addr).expect("Failed to send");
            }
        }

        let mut recv_buffer = vec![0u8; 2048];
        for _i in 0..(NUM_TEST_SENDERS * NUM_MESSAGE_TEST) as u64 {
            let prev_counter = inbound_server.counter;
            let (data, _len) = inbound_server
                .poll_message(&server_listener, &mut recv_buffer)
                .unwrap();
            //There should be no duplicates here.
            let resl = inbound_server.process_inbound_message(data).unwrap();
            assert!(inbound_server.counter > prev_counter);
            assert_eq!(inbound_server.counter, resl.as_ref().sequence_number);
            //println!("Counter is now {}", inbound_server.counter);
        }

        assert_eq!(
            inbound_server.counter,
            (NUM_TEST_SENDERS * NUM_MESSAGE_TEST) as u64
        );

        // Now send it some garbage.
        // It will have seen all of these app-sequence-numbers before
        //println!("Sending messages which should be deduplicated (using previous app
        // sequence numbers).");
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
                let client = UdpSocket::bind(&client_addr).unwrap();
                // client.connect(&server_addr).unwrap();

                // Send the archive
                client.send_to(&data, &server_addr).expect("Failed to send");
            }
        }
        for _i in 0..(NUM_TEST_SENDERS * NUM_MESSAGE_TEST) as u64 {
            let prev_counter = inbound_server.counter;
            let (data, _len) = inbound_server
                .poll_message(&server_listener, &mut recv_buffer)
                .unwrap();
            //Should always be a duplicate in this context.
            let _resl = inbound_server.process_inbound_message(data).unwrap_err();
            assert_eq!(inbound_server.counter, prev_counter);
            //println!("Counter is now {}", inbound_server.counter);
        }

        //Since it has seen all of these app sequence numbers before, the counter
        // should not have changed.
        assert_eq!(
            inbound_server.counter,
            (NUM_TEST_SENDERS * NUM_MESSAGE_TEST) as u64
        );

        //But if we send it a new one, it should recognize it.
        //println!("Lastly, sending a new message which should be new and not
        // deduplicated.");
        let example_message = "Foo, and also bar!";
        let payload = example_message.as_bytes().to_vec();
        // Build an archive
        let input = SequencerMessage::new(1_u16, 1, 2, (NUM_MESSAGE_TEST + 1) as u64, payload);
        let data = rkyv::to_bytes::<_, 256>(&input).expect("failed to serialize");

        // Start a client
        let client_addr = SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, 0, 0, 0);
        let client = UdpSocket::bind(&client_addr).unwrap();
        // client.connect(&server_addr).unwrap();

        // Send the archive
        let res = client.send_to(&data, &server_addr);
        res.expect("Failed to send");

        //println!("Processing message # {}", total_received);
        let (data, _len) = inbound_server
            .poll_message(&server_listener, &mut recv_buffer)
            .unwrap();
        let _resl = inbound_server.process_inbound_message(data).unwrap();
        //println!("Finally, counter is {}", inbound_server.counter);

        //One new valid, non-duplicate message.
        assert_eq!(
            inbound_server.counter,
            (NUM_TEST_SENDERS * NUM_MESSAGE_TEST) as u64 + 1
        );
    }

    // Generates a message record for testing purposes.
    // This is used as the first half of a couple of other tests.
    fn test_record_emit<T: MessageRecordBackend>(
        num_tests: u64,
        offset_start: u64,
        counter_start: u64,
        record: T,
    ) -> Result<T, Box<dyn std::error::Error>> {
        // Prevent tests from interfering with eachother over the localhost connection.
        // This should implicitly drop when the test ends.

        // Start a server
        let server_addr = SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, 9999, 0, 0);
        let mut server = InboundServer::new(server_addr.port(), record, offset_start, counter_start);
        //server.counter = counter_start;

        let server_listener = UdpSocket::bind(server_addr)
            .map_err(|e| SequencerError::CouldNotParse(format!("{:?}", e)))
            .unwrap();

        // Send a message from a client.

        for i in 0..num_tests {
            // Dummy message for testing.
            let example_message = format!("Hello, world! This is message #{}", i + counter_start);
            let payload = example_message.as_bytes().to_vec();
            // Build an archive
            let mut input = SequencerMessage::new(1 as AppId, 1, 2, counter_start + i, payload);
            // Give it some stupid garbage as its sequence number, so we can be sure the
            // server overwrites it.
            input.sequence_number = u64::MAX;
            //Serialize.
            let data = rkyv::to_bytes::<_, 256>(&input).expect("failed to serialize");

            // Start a client
            let client_addr = SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, 0, 0, 0);
            let client = UdpSocket::bind(&client_addr).unwrap();
            // client.connect(&server_addr).unwrap();

            // Send the archive
            client.send_to(&data, &server_addr).expect("Failed to send");
        }

        // Receive.
        let mut recv_buffer = vec![0u8; 2048];
        for _ in 0..num_tests {
            server.step(&server_listener, &mut recv_buffer).unwrap();
        }

        Ok(server.into_record())
    }

    fn msgrecord_to_messages<T: AsRef<[u8]>>(data: T) -> Vec<Vec<u8>> {
        let mut messages: Vec<Vec<u8>> = Vec::default();
        let mut cursor = 0_usize;
        while cursor < data.as_ref().len() {
            let mut length_tag_bytes = [0u8; record::LENGTH_TAG_LEN];
            length_tag_bytes
                .copy_from_slice(&data.as_ref()[cursor..cursor + record::LENGTH_TAG_LEN]);
            let length = record::LengthTag::from_le_bytes(length_tag_bytes) as usize;
            if length == 0 {
                break;
            }
            assert!(cursor + record::LENGTH_TAG_LEN + length <= data.as_ref().len());

            cursor += record::LENGTH_TAG_LEN;
            messages.push((&data.as_ref()[cursor..cursor + length]).to_vec());
            cursor += length;
        }
        messages
    }

    #[test]
    fn record_vec() {
        let _guard = IO_TEST_PERMISSIONS.lock();
        CURRENT_OFFSET.store(0, atomic::Ordering::Relaxed);
        const NUM_TESTS: usize = 32;
        let record = test_record_emit(
            NUM_TESTS as u64,
            0,
            0,
            VecBackend {
                inner: Vec::default(),
            },
        )
        .unwrap();
        drop(_guard);
        println!("File is {} bytes.", record.inner.len());

        let messages: Vec<Vec<u8>> = msgrecord_to_messages(&record.inner);

        assert_eq!(messages.len(), NUM_TESTS);

        for (i, message) in messages.iter().enumerate() {
            println!(
                "Message {} is {} bytes: {}",
                i,
                message.len(),
                hex::encode_upper(&message)
            );
            let converted = String::from_utf8_lossy(message);
            let expected_message = format!("Hello, world! This is message #{}", i);
            assert!(converted.contains(&expected_message));

            let deserialized: SequencerMessage = rkyv::from_bytes(message).unwrap();
            // App should have overwritten the sequence number gracefully.
            assert_eq!(deserialized.sequence_number, (i as u64) + 1);
            assert_eq!(deserialized.app_sequence_number, i as u64);
        }
    }

    #[test]
    fn record_mmap_file() {
        let _guard = IO_TEST_PERMISSIONS.lock();
        CURRENT_OFFSET.store(0, atomic::Ordering::Relaxed);
        const NUM_TESTS: usize = 32;

        //Ignore if it already exists.
        #[allow(unused_must_use)]
        {
            std::fs::create_dir(Path::new("test_output/"));
        }

        let path = Path::new("test_output/messagebus");

        // If a test failed, clean up after it.
        if path.exists() {
            std::fs::remove_file(path).unwrap();
        }

        let record = MemMapBackend::init(&path, 0).unwrap();
        let record = test_record_emit(NUM_TESTS as u64, 0, 0, record).unwrap();

        // Make sure the file handle gets dropped so we can open it again without
        // clobbering anything.
        drop(record);

        let mut file = std::fs::File::open(&path).unwrap();
        let mut data = Vec::default();
        file.read_to_end(&mut data).unwrap();
        drop(file);

        let messages = msgrecord_to_messages(&data);

        assert_eq!(messages.len(), NUM_TESTS);

        for (i, message) in messages.iter().enumerate() {
            println!(
                "Message {} is {} bytes: {}",
                i,
                message.len(),
                hex::encode_upper(&message)
            );
            let converted = String::from_utf8_lossy(message);
            let expected_message = format!("Hello, world! This is message #{}", i);
            assert!(converted.contains(&expected_message));

            let deserialized: SequencerMessage = rkyv::from_bytes(message).unwrap();
            // App should have overwritten the sequence number gracefully.
            assert_eq!(deserialized.sequence_number, (i as u64) + 1);
            assert_eq!(deserialized.app_sequence_number, i as u64);
        }
        //Clean up
        std::fs::remove_file(path).unwrap();
        std::fs::remove_dir(Path::new("test_output/")).unwrap();
    }

    #[test]
    fn multiple_sessions_mmap_file() {
        CURRENT_OFFSET.store(0, atomic::Ordering::Relaxed);
        const NUM_TESTS_INITIAL: usize = 32;
        let _guard = IO_TEST_PERMISSIONS.lock();

        //Ignore if it already exists.
        #[allow(unused_must_use)]
        {
            std::fs::create_dir(Path::new("test_output/"));
        }

        let path = Path::new("test_output/messagebus");

        // If a test failed, clean up after it.
        if path.exists() {
            std::fs::remove_file(path).unwrap();
        }
        #[allow(unused_assignments)]
        let mut last_offset = 0;

        {
            let record = MemMapBackend::init(&path, 0).unwrap();
            let record = test_record_emit(NUM_TESTS_INITIAL as u64, 0_u64, 0, record).unwrap();

            last_offset = record.record_len();
            // Make sure the file handle gets dropped so we can open it again without
            // clobbering anything.
            drop(record);

            let mut file = std::fs::File::open(&path).unwrap();
            let mut data = Vec::default();
            file.read_to_end(&mut data).unwrap();
            drop(file);

            let messages = msgrecord_to_messages(&data);

            assert_eq!(messages.len(), NUM_TESTS_INITIAL);

            for (i, message) in messages.iter().enumerate() {
                println!(
                    "Message {} is {} bytes: {}",
                    i,
                    message.len(),
                    hex::encode_upper(&message)
                );
                let converted = String::from_utf8_lossy(message);
                let expected_message = format!("Hello, world! This is message #{}", i);
                assert!(converted.contains(&expected_message));

                let deserialized: SequencerMessage = rkyv::from_bytes(message).unwrap();
                // App should have overwritten the sequence number gracefully.
                assert_eq!(deserialized.sequence_number, (i as u64) + 1);
                assert_eq!(deserialized.app_sequence_number, i as u64);
            }
        }
        const NUM_TESTS_SECOND: usize = 72;

        {
            let record = MemMapBackend::init(&path, last_offset).unwrap();
            let record =
                test_record_emit(NUM_TESTS_SECOND as u64, CURRENT_OFFSET.load(atomic::Ordering::Relaxed), NUM_TESTS_INITIAL as u64, record)
                    .unwrap();

            // Make sure the file handle gets dropped so we can open it again without
            // clobbering anything.
            drop(record);

            let mut file = std::fs::File::open(&path).unwrap();
            let mut data = Vec::default();
            file.read_to_end(&mut data).unwrap();
            drop(file);

            let messages = msgrecord_to_messages(&data);

            assert_eq!(messages.len(), NUM_TESTS_INITIAL + NUM_TESTS_SECOND);

            for (i, message) in messages.iter().enumerate() {
                println!(
                    "Message {} is {} bytes: {}",
                    i,
                    message.len(),
                    hex::encode_upper(&message)
                );
                let converted = String::from_utf8_lossy(message);
                let expected_message = format!("Hello, world! This is message #{}", i);
                assert!(converted.contains(&expected_message));

                let deserialized: SequencerMessage = rkyv::from_bytes(message).unwrap();
                // App should have overwritten the sequence number gracefully.
                assert_eq!(deserialized.sequence_number, (i as u64) + 1);
                assert_eq!(deserialized.app_sequence_number, i as u64);
            }
        }
        //Clean up
        std::fs::remove_file(path).unwrap();
        std::fs::remove_dir(Path::new("test_output/")).unwrap();
        CURRENT_OFFSET.store(0, atomic::Ordering::Relaxed);
    }

    #[derive(Debug, Clone)]
    enum TestError {
        CounterOutOfStep(u64, u64),
        RecvChannelError,
        SendUdpErr(String),
        BindUdpErr(String),
        SequencerErr(String),
    }

    impl Display for TestError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                TestError::CounterOutOfStep(expected, actual) => write!(
                    f,
                    "The counter did not increase monotonically! We expected the counter to be at {} and instead it was {}",
                    expected,
                    actual,
                ),
                TestError::RecvChannelError => write!(
                    f,
                    "Error receiving on a message-passing channel"
                ),
                TestError::SendUdpErr(underlying) => write!(
                    f,
                    "Error sending UDP packet: {}",
                    underlying,
                ),
                TestError::BindUdpErr(underlying) => write!(
                    f,
                    "Error binding to UDP socket: {}",
                    underlying,
                ),
                TestError::SequencerErr(err) => write!(
                    f,
                    "Sequencer logic error: {}",
                    err,
                ),
            }
        }
    }
    impl std::error::Error for TestError {}

    #[test]
    pub fn test_offset_counter_consistency() {
        let _guard = IO_TEST_PERMISSIONS.lock();
        const NUM_TESTS: usize = 32;
        CURRENT_OFFSET.store(0, atomic::Ordering::Relaxed);
        let record = VecBackend{inner: Vec::new()};

        let runtime = build_async_runtime().unwrap();

        let mut counter_change_receiver = offset_watcher_thread(4096, Duration::from_millis(1)); 

        // Channels
        let (counter_error_sender, mut counter_error_receiver): (
            Sender<TestError>,
            Receiver<TestError>,
        ) = tokio::sync::broadcast::channel(NUM_TESTS);
        let (end_test_sender, mut end_test_receiver): (Sender<()>, Receiver<()>) =
            tokio::sync::broadcast::channel(NUM_TESTS);

        let end_test_sender_clone = end_test_sender.clone();

        let err_sender_clone = counter_error_sender.clone();
        let mut err_receiver_clone = counter_error_sender.subscribe();
        let mut end_test_receiver_clone = end_test_sender.subscribe();

        // TODO: Find a more graceful, less gross way to do this. 
        // Prevent various race conditions.
        let _end_tester_keepalive = end_test_sender.clone();
        //Retain the channel even when the server object gets dropped, so the test
        // doesn't error when it's shutting down.
        let _counter_change_keepalive = counter_change_receiver.resubscribe();
        let _err_sender_keepalive = counter_error_sender.clone();

        // Counter-checking task:
        let join_handle_1 = runtime.spawn( async move {
            let mut prev_offset = CURRENT_OFFSET.load(atomic::Ordering::Relaxed);
            loop {
                tokio::select! {
                    _ = end_test_receiver_clone.recv() => {
                        break;
                    }
                    len_maybe = counter_change_receiver.recv() => {
                        match len_maybe {
                            Ok(new_byte_len) => {
                                let new_offset = CURRENT_OFFSET.load(atomic::Ordering::Relaxed);

                                if new_offset <= prev_offset { 
                                    err_sender_clone.send(TestError::CounterOutOfStep(prev_offset + new_byte_len, new_offset)).unwrap();
                                    break;
                                }
                                prev_offset = new_offset; 
                            }, 
                            Err(_e) => {
                                err_sender_clone.send(TestError::RecvChannelError).unwrap();
                            }
                        }
                    },
                }
            }
            end_test_sender.send(()).unwrap();
        });

        // Message-generating UDP task
        let join_handle_2 = runtime.spawn(async move {
            // Start a server
            let server_addr = SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, 9999, 0, 0);

            let mut server = InboundServer::new(server_addr.port(), record, 0, 0);

            let server_listener = UdpSocket::bind(server_addr)
                .map_err(|e| SequencerError::CouldNotParse(format!("{:?}", e)))
                .unwrap();

            let mut recv_buffer = vec![0u8; 2048];

            for i in 0..NUM_TESTS {
                //println!("Sending message {}", i);
                // Dummy message for testing.
                let example_message = format!("Hello, world! This is message #{}", i);
                let payload = example_message.as_bytes().to_vec();
                // Build an archive
                let mut input = SequencerMessage::new(1 as AppId, 1, 2, i as u64, payload);
                // Give it some stupid garbage as its sequence number, so we can be sure the
                // server overwrites it.
                input.sequence_number = u64::MAX;
                //Serialize.
                let data = rkyv::to_bytes::<_, 256>(&input).expect("failed to serialize");

                // Start a client
                let client_addr = SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, 0, 0, 0);
                let client = match UdpSocket::bind(&client_addr) {
                    Ok(client) => client,
                    Err(e) => {
                        counter_error_sender
                            .send(TestError::BindUdpErr(format!("{:?}", e)))
                            .unwrap();
                        break;
                    }
                };

                // Send the archive
                match client.send_to(&data, &server_addr) {
                    Ok(_) => {}
                    Err(e) => {
                        counter_error_sender
                            .send(TestError::SendUdpErr(format!("{:?}", e)))
                            .unwrap();
                        break;
                    }
                }

                // Receive.
                match server.step(&server_listener, &mut recv_buffer) {
                    Ok(()) => {}
                    Err(e) => {
                        counter_error_sender
                            .send(TestError::SequencerErr(format!("{:?}", e)))
                            .unwrap();
                    }
                }

                // Was the test ended by an error elsewhere? If so, break out of the loop.
                match err_receiver_clone.try_recv() {
                    Ok(inner_error) => {
                        println!(
                            "Ending test message-sending thread due to error: {:?}",
                            inner_error
                        );
                        break;
                    }
                    Err(tokio::sync::broadcast::error::TryRecvError::Empty) => {
                        /* No error yet, do nothing */
                    }
                    Err(_) => {
                        println!(
                            "Cannot receive on error channel, it must have no senders remaining."
                        );
                        break;
                    }
                }
            }
            end_test_sender_clone.send(()).unwrap();
            server.into_record()
        });

        let resl: Result<(), TestError> = runtime.block_on(async move {
            tokio::select! {
                some_kind_of_err = counter_error_receiver.recv() => {
                    match some_kind_of_err { 
                        Ok(err) => Err(err), 
                        Err(_e) => panic!("Could not receive an error from the error channel."),
                    }
                }
                _end = end_test_receiver.recv() => {
                    //Test completed successfully, there are no problems.
                    Ok(())
                }
            }
        });
        resl.unwrap();
        runtime.block_on(join_handle_1).unwrap();
        let record = runtime.block_on(join_handle_2).unwrap();

        //Check to see if the number is right.
        let offset = CURRENT_OFFSET.load(atomic::Ordering::Relaxed);

        assert_eq!(record.inner.len() as u64, offset);
    }
}
