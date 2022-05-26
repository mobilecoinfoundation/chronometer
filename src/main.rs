use tokio;
use tokio::io::{AsyncWriteExt, AsyncReadExt};
use tokio::net::{TcpListener, TcpStream};
use clap::Parser;
use std::net::{
    Ipv6Addr, SocketAddrV6
};
use flatbuffers::FlatBufferBuilder;
use byteorder::{ByteOrder, NetworkEndian, NativeEndian};

#[path = "../target/flatbuffers/unsequenced_generated.rs"]
pub mod unsequenced_generated;
use unsequenced_generated::unsequenced::*;
use crate::root_as_unsequenced_input;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(short, long, default_value_t = 1)]
    instance: u16,
    #[clap(short, long, default_value_t = 0)]
    port: u16
}

struct InboundConnection {
    socket: TcpStream
}

impl InboundConnection {
    pub fn new(socket: TcpStream) -> InboundConnection {
        InboundConnection {
            socket
        }
    }
    
    pub async fn start(mut self) -> std::io::Result<()> {
        loop {
            if let Err(e) = self.receive_framed().await {
                eprintln!("connection closed: {:?}", e);
                return Err(e);
            }
        }
    }
    
    async fn receive_framed(&mut self) -> std::io::Result<()> {
        let mut buf = [0; flatbuffers::SIZE_SIZEPREFIX];
        let n = match self.socket.read_exact(&mut buf).await {
            Ok(n) if n == 0 => return Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof)),
            Ok(n) => n,
            Err(e) => {
                eprintln!("failed to read from socket, error {:?}", e);
                return Err(e);
            }
        };
        assert!(flatbuffers::SIZE_SIZEPREFIX == flatbuffers::SIZE_U32);
        //let length = NetworkEndian::read_u32(&buf);
        let length = NativeEndian::read_u32(&buf);
        println!("received {}({}) bytes, {:?}", length, n, buf);
        
        let mut buffer = vec![0; length as usize];
        let n = match self.socket.read_exact(&mut buffer).await {
            Ok(n) if n == 0 => return Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof)),
            Ok(n) => n,
            Err(e) => {
                eprintln!("failed to read from socket, error {:?}", e);
                return Err(e);
            }
        };
        
        println!("received {}({}) bytes, {:?}", buffer.len(), n, buffer);
        
        let res = unsequenced_generated::unsequenced::root_as_unsequenced_input(&buffer);
 
        println!("{:?}", res);
        
        Ok(())
    }
}

struct InboundConnectionServer {
    port: u16
}

impl InboundConnectionServer {
    pub fn new(port: u16) -> InboundConnectionServer {
        InboundConnectionServer {
            port
        }
    }
    
    pub async fn start(self) -> std::io::Result<()> {
        let addr = SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, self.port, 0, 0);
        let listener = TcpListener::bind(addr).await?;
        
        println!("Hello, world on port {:?}!", listener);
        loop {
            let (socket, _) = listener.accept().await.unwrap();
            println!("{:?}", socket);
            let inbound_connection = InboundConnection::new(socket);
            tokio::spawn(inbound_connection.start());
        }
    }
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let args = Args::parse();
    
    let inbound_server = InboundConnectionServer::new(args.port);
    let join_handle = tokio::spawn(inbound_server.start());
    
    join_handle.await?
}

#[tokio::test(flavor = "multi_thread")]
async fn test_streaming() {
    // Build a flatbuffer
    let mut builder = FlatBufferBuilder::new();
    let mut dest: Vec<u8> = vec![];
    
    let args = UnsequencedInputArgs {
        app_id: 1,
        instance_id: 2,
        cluster_id: 3,
        sequence_number: 4,
        payload: None
    };
    
    let offset = UnsequencedInput::create(&mut builder, &args);
    finish_size_prefixed_unsequenced_input_buffer(&mut builder, offset);
    
    dest.extend_from_slice(builder.finished_data());
    
    // Start a server
    let inbound_server = InboundConnectionServer::new(9999);
    let join_handle = tokio::spawn(inbound_server.start());

    // Start a client
    let addr = SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, 9999, 0, 0);
    let mut client = tokio::net::TcpStream::connect(&addr).await.unwrap();

    // Send the flatbuffer
    client.write_all(&dest).await.unwrap();
    
    join_handle.await;
}
