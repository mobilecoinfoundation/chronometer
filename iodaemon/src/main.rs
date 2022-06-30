use clap::Parser;
use log::info;
use nix::{
    fcntl::{splice, SpliceFFlags},
    unistd::pipe,
};
use std::{
    env::current_exe,
    fmt::{Display, Formatter},
    fs::File,
    io::{ErrorKind, Read, Write},
    net::{IpAddr, SocketAddr, TcpStream},
    os::unix::io::AsRawFd,
    path::PathBuf,
    str::FromStr,
};

#[derive(Debug)]
enum Backend {
    Splice,
    Memcpy,
}

impl Display for Backend {
    fn fmt(&self, f: &mut Formatter) -> Result<(), std::fmt::Error> {
        match *self {
            Backend::Splice => write!(f, "Splice"),
            Backend::Memcpy => write!(f, "Memcpy"),
        }
    }
}

#[derive(Debug)]
enum BackendParseError {
    VariantNotFound,
}

impl Display for BackendParseError {
    fn fmt(&self, f: &mut Formatter) -> Result<(), std::fmt::Error> {
        match *self {
            BackendParseError::VariantNotFound => write!(f, "VariantNotFound"),
        }
    }
}

impl std::error::Error for BackendParseError {
    fn description(&self) -> &str {
        match self {
            &BackendParseError::VariantNotFound => {
                "Unable to find a backend matching the string given."
            }
        }
    }
}

impl FromStr for Backend {
    type Err = BackendParseError;
    fn from_str(x: &str) -> Result<Self, <Self as FromStr>::Err> {
        match &*x.to_lowercase() {
            "splice" => Ok(Backend::Splice),
            "memcpy" => Ok(Backend::Memcpy),
            _ => Err(BackendParseError::VariantNotFound),
        }
    }
}

use crate::Backend::Splice;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(short, long, env)]
    ip_address: IpAddr,
    #[clap(short, long, env)]
    port: u16,
    #[clap(short, long, env)]
    output: Option<PathBuf>,
    #[clap(short, long, default_value_t = Splice)]
    backend: Backend,
}

fn main() -> std::io::Result<()> {
    let args = Args::parse();

    // open the output file
    let output_path = if args.output.is_some() {
        args.output.unwrap()
    } else {
        let mut buf = PathBuf::from("/var/run/");
        buf.push(
            current_exe()?
                .file_name()
                .ok_or_else(|| std::io::Error::from(ErrorKind::InvalidData))?,
        );
        buf.push("output");
        buf
    };

    info!("outputting to file at {}", output_path.display());

    if let Some(directory) = output_path.as_path().parent() {
        std::fs::create_dir_all(directory)?;
    }

    let mut output_file = File::options()
        .write(true)
        .create_new(true)
        .open(output_path)?;

    // connect to the sequencer server
    let sequencer_output = SocketAddr::new(args.ip_address, args.port);
    let mut std_socket = TcpStream::connect(&sequencer_output)?;

    const BUF_SIZE: usize = 4096;

    match args.backend {
        Backend::Splice => {
            info!("{}", "creating pipe");
            // create intermediate pipe
            let (pipe_out, pipe_in) = pipe()?;

            loop {
                // splice from socket to pipe
                let to_pipe = splice(
                    std_socket.as_raw_fd(),
                    None,
                    pipe_in,
                    None,
                    BUF_SIZE,
                    SpliceFFlags::SPLICE_F_MOVE,
                )?;
                if to_pipe == 0 {
                    // socket has closed
                    return Err(std::io::Error::from(ErrorKind::ConnectionAborted));
                }
                // splice from pipe to file
                let from_pipe = splice(
                    pipe_out,
                    None,
                    output_file.as_raw_fd(),
                    None,
                    to_pipe,
                    SpliceFFlags::SPLICE_F_MOVE,
                )?;

                assert!(to_pipe == from_pipe);
            }
        }
        Backend::Memcpy => {
            loop {
                let mut buffer = [0; BUF_SIZE];
                // read from socket
                let received = std_socket.read(&mut buffer)?;

                if received == 0 {
                    // socket has closed
                    return Err(std::io::Error::from(ErrorKind::ConnectionAborted));
                }

                // write to file
                let written = output_file.write(&buffer[..received])?;

                assert!(received == written);
            }
        }
    }
}
