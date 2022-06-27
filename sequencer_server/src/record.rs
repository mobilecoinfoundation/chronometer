//! A record / history of all messages sent on this consistently-sequenced
//! message bus. The sequencer's message history is split into two memory-mapped
//! ring buffer files, one of which contains the current

use memmap2::{MmapMut, MmapOptions, Mmap};

//use rkyv::validation::CheckArchiveError;
use sequencer_common::{LengthTag, AppId, SequencerMessage};
use tokio::io::{AsyncWrite, AsyncWriteExt}; 

use std::{
    fs::{File, OpenOptions},
    io::{Seek, SeekFrom, Write},
    path::Path, fmt::Display,
};

/// How many bytes should we add to the file when it's time to grow the file?
const GROW_BY: usize = 65535;

pub trait MessageRecordBackend: Sized {
    fn init<T: AsRef<Path>>(file_path: T, start_offset: u64) -> Result<Self, std::io::Error>;
    fn write_message<T: AsRef<[u8]>>(&mut self, message: T) -> Result<u64, std::io::Error>;
    /// How long was this file when we opened it, at the start of this run of
    /// the program?
    fn initial_offset(&self) -> u64;
    /// How many bytes total comprise this record?
    fn record_len(&self) -> u64;
}

pub struct MemMapBackend {
    file: File,
    //mmap: MmapMut, TODO: Find a way to make mmap growable
    start_offset: u64,
    //How long is the actual data we have stored?
    current_offset: u64,
    //How big is the file right now? (i.e. current_record_len plus empty space)
    current_file_len: u64,
    //Resets to 0 every time we grow the file - needed to find where we are in the memmap slice.
    mmap_cursor: usize,
    mmap: MmapMut,
}

impl MemMapBackend {
    #[inline]
    fn grow_if_needed(&mut self, additional_bytes_len: u64) -> Result<(), std::io::Error> {
        if (self.current_offset + additional_bytes_len) > self.current_file_len {
            let to_grow = GROW_BY.max(additional_bytes_len as usize);
            let new_len = self.current_file_len + to_grow as u64;

            #[cfg(debug_assertions)]
            {
                println!("Growing file to {}...", new_len);
            }

            // Expand file.
            self.file.set_len(new_len)?;
            self.file.seek(SeekFrom::Start(self.current_file_len))?;

            // Zeroize the next length tag (null terminate) and don't advance the cursor or
            // current offset (so it can get overwritten)
            let zeroes = [0u8; std::mem::size_of::<LengthTag>()];
            self.file.write_all(&zeroes)?;
            self.file.seek(SeekFrom::Start(self.current_file_len))?;

            self.current_file_len = new_len;
            self.mmap = {
                let mut options = MmapOptions::default();
                // Append - start at the end of the file.
                options.offset(self.current_offset);
                options.len(new_len as usize - self.current_offset as usize);

                #[cfg(debug_assertions)]
                {
                    println!(
                        "Memory map will start at {} and be {} bytes long",
                        self.current_offset, to_grow
                    );
                }

                // There is, apparently, no such thing as a "safe" memory map
                // since, at an OS level, not much safety is built in -
                // other processes can change them out from under you.
                unsafe { options.map_mut(&self.file)? }
            };
            self.mmap_cursor = 0;
        }
        Ok(())
    }
    pub fn get_file_len(&self) -> u64 {
        self.current_file_len
    }
}

impl MessageRecordBackend for MemMapBackend {
    fn init<T: AsRef<Path>>(file_path: T, start_offset: u64) -> Result<Self, std::io::Error> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .append(true)
            .create(true)
            .open(file_path)?;

        let file_len = file.seek(SeekFrom::End(0))?;

        if file_len == 0 {
            file.set_len(GROW_BY as u64)?;
        }
        let file_len = file.seek(SeekFrom::End(0))?;
        file.seek(SeekFrom::Start(0))?;

        let start_offset = if start_offset > file_len {
            println!("WARNING: Attempting to start at an offset which the message bus has not reached yet. Defaulting to end of file.");
            eprintln!("WARNING: Attempting to start at an offset which the message bus has not reached yet. Defaulting to end of file.");
            file_len
        } else {
            start_offset
        };

        let mut options = MmapOptions::default();
        // Append - start at the end of the file.
        options.offset(start_offset);

        // There is, apparently, no such thing as a "safe" memory map
        // since, at an OS level, not much safety is built in -
        // other processes can change them out from under you.
        let mmap = unsafe { options.map_mut(&file)? };

        /*
        #[cfg(unix)]
        {
            // This is a Unix-only feature so there is no
            // perf hint on, for example, Azure.
            mmap.advise(memmap2::Advice::Sequential)?;
        }*/

        Ok(Self {
            file,
            start_offset,
            current_offset: start_offset,
            current_file_len: file_len,
            mmap_cursor: 0,
            mmap,
        })
    }

    #[inline]
    fn write_message<T: AsRef<[u8]>>(&mut self, message: T) -> Result<u64, std::io::Error> {
        // Length of message payload to append.
        let message_len = message.as_ref().len() as usize;
        assert!(message_len < LengthTag::MAX as usize);
        // Total new bytes to append.
        let additional_len = (std::mem::size_of::<LengthTag>() + message_len) as usize;
        // End of length prefix, start of message payload.
        //let message_start = self.current_offset + LENGTH_PREFIX_LEN as u64;
        // Length prefix bytes.
        // TODO: Discuss with team - are we sure we want little-endian?
        let len_bytes = (message.as_ref().len() as LengthTag).to_le_bytes();
        let full_len = std::mem::size_of::<LengthTag>() + message_len as usize;
        // How long will the file need to be to contain this new information?
        let new_record_end = self.current_offset + additional_len as u64;

        // Grow the file and associated memmap
        self.grow_if_needed(additional_len as u64)?;

        /*#[cfg(unix)]
        {
            // This is a Unix-only feature so there is no
            // perf hint on, for example, Azure.
            if let Err(e) = mmap.advise(memmap2::Advice::Sequential) {
                return future::err(e);
            }
        }*/

        // Current range of the memory map right now appears to be offset..len - this
        // affects how the slice works. mmap[self.current_offset as usize ..
        // message_start as usize]    .copy_from_slice(&len_bytes);
        //mmap[message_start as usize .. new_file_end as usize]
        //    .copy_from_slice(message.as_ref());

        // Push our length prefix.
        self.mmap[self.mmap_cursor..self.mmap_cursor + std::mem::size_of::<LengthTag>()].copy_from_slice(&len_bytes);
        // Push our message.
        self.mmap[self.mmap_cursor + std::mem::size_of::<LengthTag>()..self.mmap_cursor + full_len]
            .copy_from_slice(message.as_ref());

        let mut written_len = full_len;

        // Zeroize the next length tag, and don't advance the cursor or current offset
        // (so it can get overwritten)
        if (self.current_offset + full_len as u64 + std::mem::size_of::<LengthTag>() as u64) > self.current_file_len {
            let zeroes = [0u8; std::mem::size_of::<LengthTag>()];
            self.mmap[self.mmap_cursor + full_len..self.mmap_cursor + full_len + std::mem::size_of::<LengthTag>()]
                .copy_from_slice(&zeroes);
            written_len += std::mem::size_of::<LengthTag>();
        }

        self.mmap.flush_range(self.mmap_cursor, written_len)?;

        // Update our current offset counter.
        self.current_offset = new_record_end;
        self.mmap_cursor += written_len;

        Ok(written_len as u64)
    }
    fn initial_offset(&self) -> u64 {
        self.start_offset
    }
    fn record_len(&self) -> u64 {
        self.current_offset
    }
}

#[derive(Debug, Clone)]
pub enum RecordReadError {
    InvalidLength(usize, usize, usize)
}

impl Display for RecordReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RecordReadError::InvalidLength(message_length, current_cursor, file_end) => write!(
                f,
                "Attempted to read a message of length {}, starting from position {}, which would take us past the end of the file which is at position {}",
                message_length, 
                current_cursor, 
                file_end,
            ),
        }
    }
}

impl std::error::Error for RecordReadError {}

pub struct RecordReader<'a> {
    cursor: usize,
    data: &'a [u8],
}

impl<'a> RecordReader<'a> {
    pub fn new(data: &'a [u8]) -> Self { 
        Self {
            cursor: 0, 
            data 
        }
    }
    pub fn get_cursor(&self) -> usize { 
        self.cursor
    }
    /// Returns Ok(None) if we've reached the end of our record. 
    pub fn get_next(&mut self) ->  Result<Option<&'a [u8]>, RecordReadError> { 
        let mut length_tag_bytes = [0u8; std::mem::size_of::<LengthTag>()];
        length_tag_bytes.copy_from_slice(&self.data[self.cursor..self.cursor + std::mem::size_of::<LengthTag>()]);
        let message_length = LengthTag::from_le_bytes(length_tag_bytes) as usize;
        //length tag specifying zero is EOF
        if message_length == 0 {
            return Ok(None);
        }
        if self.cursor + std::mem::size_of::<LengthTag>() + message_length > self.data.len() { 
            return Err(RecordReadError::InvalidLength( message_length + std::mem::size_of::<LengthTag>(), self.cursor, self.data.len()));
        }

        self.cursor += std::mem::size_of::<LengthTag>();
        let message = &self.data[self.cursor..self.cursor+message_length];
        self.cursor += message_length;

        Ok(Some(message))
    }

}

impl<'a> Iterator for RecordReader<'a> { 
    type Item = Result<&'a [u8], RecordReadError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.get_next() { 
            Ok(Some(msg)) => Some(Ok(msg)),
            Ok(None) => None, 
            Err(e) => Some(Err(e)),
        }
    }
}

#[derive(Debug)]
pub enum MmapReadError {
    /// Record read issue, probably related to length tag. 
    RecordReadLogic(RecordReadError),
    IoError(std::io::Error),
    CheckArchive(String),
}

impl Display for MmapReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MmapReadError::RecordReadLogic(e) => write!(
                f,
                "{:?}",
                e,
            ),
            MmapReadError::IoError(e) => write!(
                f, 
                "Could not read a memory-mapped message record due to io error: {:?}",
                e
            ),
            MmapReadError::CheckArchive(e) => write!(
                f, 
                "Failed to validate message structure when parsing to check app ID: {}",
                e
            ),
        }
    }
}
impl std::error::Error for MmapReadError {}

impl From<RecordReadError> for MmapReadError {
    fn from(e: RecordReadError) -> Self {
        MmapReadError::RecordReadLogic(e)
    }
}
impl From<std::io::Error> for MmapReadError {
    fn from(e: std::io::Error) -> Self {
        MmapReadError::IoError(e)
    }
}

pub struct MemMapRecordReader {
    pub(in crate::record) mmap: Mmap,
    /// How far, in absolute terms, have we got in this epoch's global byte stream? 
    pub(in crate::record) start_offset: u64,
    /// How far has this reader gotten into the mmap?
    /// NOTE - mmap 0 starts at start_offset. This does not correspond to a global stream index. 
    pub(in crate::record) last_read_offset: u64,
}

// No remap option because the behavior of MemMapRecordReader.new (file, old.most_recent_offset_visited()) 
// would be identical to what remap would do anyway.

impl MemMapRecordReader {
    pub fn new<T: AsRef<Path>>(file_path: T, start_offset: u64) -> Result<Self, std::io::Error> { 
        let mut file = OpenOptions::new()
            .read(true)
            .write(false)
            .open(file_path)?;

        let file_len = file.seek(SeekFrom::End(0))?;

        let start_offset = if start_offset > file_len {
            println!("WARNING: Attempting to start at an offset which the message bus has not reached yet. Defaulting to 0.");
            eprintln!("WARNING: Attempting to start at an offset which the message bus has not reached yet. Defaulting to 0.");
            file_len
        } else {
            start_offset
        };

        file.seek(SeekFrom::Start(start_offset))?;

        let mut options = MmapOptions::default();
        options.offset(start_offset);

        let mmap = unsafe { options.map(&file)? };

        Ok(Self {
            mmap,
            start_offset,
            last_read_offset: 0, //Remember, it's in mmap-space, not global-space.
        })
    }

    /// Attempt to read messages from between the last index we left off on, and self.mmap.len(),
    /// checking to see if app_id matches one of the IDs on the provided array and only pushing if it does.
    /// 
    /// Providing an empty set of app IDs causes this object to send regardless / ignore the message's app ID. 
    /// 
    /// This method should be fully zero-copy, end-to-end, unless the OS performs its own copy when we 
    /// push to the socket.
    pub async fn read_and_push_to<W: AsyncWrite + AsyncWriteExt + Unpin>(&mut self, app_ids: &[AppId], writer: &mut W) -> Result<u64, MmapReadError> { 
        let mut reader = RecordReader::new(&self.mmap.as_ref()[self.last_read_offset as usize ..]);
        while let Some(maybe_message) = reader.next() {
            let message = maybe_message?;

            if app_ids.is_empty() { 
                // No whitelist, match all. 
                // Length tag 
                writer.write_u16_le(message.len() as u16).await?;
                // Write the message.
                writer.write_all(message).await?;
            }
            else {
                rkyv::check_archived_root::<SequencerMessage>(message)
                    .map_err(|e| MmapReadError::CheckArchive(format!("{:?}", e)))?;

                let data = unsafe {
                    rkyv::archived_root::<SequencerMessage>(message)
                };
                if app_ids.contains(&data.app_id) { 
                    // Length tag 
                    writer.write_u16_le(message.len() as u16).await?;
                    // Write the message.
                    writer.write_all(message).await?;
                }
            }
        }
        let amt_written = reader.cursor as u64 - self.last_read_offset;
        self.last_read_offset += reader.cursor as u64;
        Ok(amt_written)
    }
    /// This mostly exists for tests - in production you will, generally, want to use read_and_push_to() as that is zero-copy. 
    pub fn read_all(&mut self) -> Result<Vec<Vec<u8>>, MmapReadError>  {
        let mut all_messages = Vec::new();
        let mut reader = RecordReader::new(&self.mmap.as_ref()[self.last_read_offset as usize ..]);
        while let Some(maybe_message) = reader.next() {
            let message = maybe_message?; 
            all_messages.push(message.to_vec());
        }
        self.last_read_offset += reader.cursor as u64;
        Ok(all_messages)
    }
    /// How far in terms of global stream offset (total bytes sent this epoch) have we gotten? 
    pub fn most_recent_offset_visited(&self) -> u64 { 
        self.last_read_offset + self.start_offset
    }
    /// Where in the global stream offset (total bytes sent this epoch) does our currently-mapped range end? 
    pub fn range_end(&self) -> u64 { 
        self.start_offset + self.mmap.as_ref().len() as u64 
    }
}

#[cfg(test)]
pub mod test_util {
    use super::{LengthTag, MessageRecordBackend};

    pub struct DummyBackend {}

    impl MessageRecordBackend for DummyBackend {
        #[allow(unused_variables)]
        fn init<T: AsRef<std::path::Path>>(
            file_path: T,
            start_offset: u64,
        ) -> Result<Self, std::io::Error> {
            Ok(DummyBackend {})
        }

        #[allow(unused_variables)]
        fn write_message<T: AsRef<[u8]>>(&mut self, message: T) -> Result<u64, std::io::Error> {
            Ok(0)
        }

        fn initial_offset(&self) -> u64 {
            0
        }
        fn record_len(&self) -> u64 {
            0
        }
    }

    pub struct VecBackend {
        pub inner: Vec<u8>,
    }

    impl MessageRecordBackend for VecBackend {
        #[allow(unused_variables)]
        fn init<T: AsRef<std::path::Path>>(
            file_path: T,
            start_offset: u64,
        ) -> Result<Self, std::io::Error> {
            Ok(VecBackend {
                inner: Vec::default(),
            })
        }

        #[allow(unused_variables)]
        fn write_message<T: AsRef<[u8]>>(&mut self, message: T) -> Result<u64, std::io::Error> {
            // Length-prefixing.
            let len_bytes = (message.as_ref().len() as LengthTag).to_le_bytes();
            self.inner.extend_from_slice(&len_bytes);
            // Write message.
            self.inner.extend_from_slice(message.as_ref());
            Ok((len_bytes.len() + message.as_ref().len()) as u64)
        }

        fn initial_offset(&self) -> u64 {
            0
        }
        fn record_len(&self) -> u64 {
            self.inner.len() as u64
        }
    }

    pub struct MultiPushBackend {
        pub sender: tokio::sync::broadcast::Sender<Vec<u8>>,
        pub receiver: tokio::sync::broadcast::Receiver<Vec<u8>>,
        pub count: u64,
    }

    impl MultiPushBackend {
        pub fn new(capacity: usize) -> Self {
            let (sender, receiver) = tokio::sync::broadcast::channel(capacity);
            MultiPushBackend {
                sender,
                receiver,
                count: 0,
            }
        }
    }

    impl MessageRecordBackend for MultiPushBackend {
        #[allow(unused_variables)]
        fn init<T: AsRef<std::path::Path>>(
            file_path: T,
            start_offset: u64,
        ) -> Result<Self, std::io::Error> {
            Ok(MultiPushBackend::new(4096))
        }

        #[allow(unused_variables)]
        fn write_message<T: AsRef<[u8]>>(&mut self, message: T) -> Result<u64, std::io::Error> {
            let mut buf_make = Vec::new();

            if message.as_ref().is_empty() {
                return Ok(0);
            }
            // Length-prefixing.
            let len_bytes = (message.as_ref().len() as LengthTag).to_le_bytes();

            // Write message.
            buf_make.extend_from_slice(&len_bytes);
            self.count += len_bytes.len() as u64;
            // Write message.
            buf_make.extend_from_slice(message.as_ref());
            self.count += message.as_ref().len() as u64;

            let len_written = buf_make.len();

            self.sender.send(buf_make).unwrap();

            Ok(len_written as u64)
        }

        fn initial_offset(&self) -> u64 {
            0
        }
        fn record_len(&self) -> u64 {
            self.count
        }
    }
}
