use clap::Parser;
use std::net::{Ipv6Addr, SocketAddrV6};
use tokio::{self, net::UdpSocket};
use unsequenced_producer::UnsequencedInput;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(short, long, default_value_t = 1)]
    instance: u16,
    #[clap(short, long, default_value_t = 0)]
    port: u16,
}

struct InboundServer {
    port: u16,
}

impl InboundServer {
    pub fn new(port: u16) -> InboundServer {
        InboundServer { port }
    }

    pub async fn start(self) -> std::io::Result<()> {
        let addr = SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, self.port, 0, 0);
        let listener = UdpSocket::bind(addr).await?;

        println!("Hello, world on port {:?}!", listener);
        let mut recv_buffer = vec![0u8; 2048];
        loop {
            let (len, addr) = listener.recv_from(&mut recv_buffer).await?;
            println!("{}, {:?}", len, addr);
            let data_res = rkyv::check_archived_root::<UnsequencedInput>(&recv_buffer[..len]);

            if let Ok(data) = data_res {
                println!("Received unsequenced input: {:?}", data);
            } else {
                println!("Received error: {:?}", data_res);
            }
        }
    }
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let args = Args::parse();

    let inbound_server = InboundServer::new(args.port);
    let join_handle = tokio::spawn(inbound_server.start());

    join_handle.await?
}

#[tokio::test(flavor = "multi_thread")]
async fn test_streaming() {
    // Build an archive
    let input = UnsequencedInput::new(1, 2, 3, 4, vec![]);

    let data = rkyv::to_bytes::<_, 256>(&input).expect("failed to serialize");

    println!("data is: {:?}", data);

    // Start a server
    let server_addr = SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, 9999, 0, 0);
    let inbound_server = InboundServer::new(server_addr.port());
    let join_handle = tokio::spawn(inbound_server.start());

    // Start a client
    let client_addr = SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, 0, 0, 0);
    let client = tokio::net::UdpSocket::bind(&client_addr).await.unwrap();
    println!("client is at {:?}", client.local_addr());
    // client.connect(&server_addr).await.unwrap();

    // Send the flatbuffer
    let res = client.send_to(&data, &server_addr).await;
    println!("{:?}", res);

    join_handle.await.unwrap().unwrap();
}
