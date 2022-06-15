//! A record / history of all messages sent on this consistently-sequenced
//! message bus. The sequencer's message history is split into two memory-mapped
//! ring buffer files, one of which contains the current

use std::{future::Future, path::Path, fs::{File, OpenOptions}, io::{SeekFrom, Seek}};

use futures::future;
use memmap2::MmapOptions;

pub trait MessageRecordBackend : Sized {
    type WriteMsgFuture: Future<Output=Result<(), std::io::Error>>; 

    fn init<T: AsRef<Path>>(file_path: T) -> Result<Self, std::io::Error>; 
    fn write_message<T: AsRef<[u8]>>(&mut self, message: T) -> Self::WriteMsgFuture; 
    /// How long was this file when we opened it, at the start of this run of the program? 
    fn initial_offset(&self) -> u64;
    /// How many bytes total comprise this record? 
    fn record_len(&self) -> u64;
}

pub struct MemMapBackend { 
    file: File,
    //mmap: MmapMut, TODO: Find a way to make mmap growable
    start_offset: u64,
    current_offset: u64, 
}

impl MessageRecordBackend for MemMapBackend {
    type WriteMsgFuture = futures::future::Ready<Result<(), std::io::Error>>;

    fn init<T: AsRef<Path>>(file_path: T) -> Result<Self, std::io::Error> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .append(true)
            .create(true)
            .open(file_path)?;
        
        let file_len = file.seek(SeekFrom::End(0))?;

        /*
        let mut options = MmapOptions::default();
        // Append - start at the end of the file. 
        options.offset(file_len);

        // There is, apparently, no such thing as a "safe" memory map
        // since, at an OS level, not much safety is built in -
        // other processes can change them out from under you.
        let mmap = unsafe { options.map_mut(&file)? };

        #[cfg(unix)]
        { 
            // This is a Unix-only feature so there is no 
            // perf hint on, for example, Azure.
            mmap.advise(memmap2::Advice::Sequential)?;
        }
*/
        Ok(
            Self {
                file,
                start_offset: file_len,
                current_offset: file_len,
            }
        )
    }

    #[inline]
    fn write_message<T: AsRef<[u8]>>(&mut self, message: T) -> Self::WriteMsgFuture {
        // Length of message payload to append.
        let message_len = message.as_ref().len() as u64; 
        // Total new bytes to append.
        let additional_len: u64 = std::mem::size_of::<u64>() as u64 + message_len;
        // End of length prefix, start of message payload.
        let message_start = self.current_offset + std::mem::size_of::<u64>() as u64;
        // Length prefix bytes.
        // TODO: Discuss with team - are we sure we want little-endian? 
        let len_bytes = (message.as_ref().len() as u64).to_le_bytes();
        // How long will the file need to be to contain this new information?
        let new_file_end = self.current_offset + additional_len;

        // Expand file.
        match self.file.set_len(new_file_end) {
            Ok(_) => { /* continue */},  
            Err(e) => return future::err(e.into()),
        };

        let mut options = MmapOptions::default();
        // Append - start at the end of the file. 
        options.offset(self.current_offset);

        // There is, apparently, no such thing as a "safe" memory map
        // since, at an OS level, not much safety is built in -
        // other processes can change them out from under you.
        let mut mmap = unsafe { 
            match options.map_mut(&self.file) {
                Ok(m) => m, 
                Err(e) => return future::err(e),   
            } 
        };

        /*#[cfg(unix)]
        { 
            // This is a Unix-only feature so there is no 
            // perf hint on, for example, Azure.
            if let Err(e) = mmap.advise(memmap2::Advice::Sequential) {
                return future::err(e);
            }
        }*/

        // Push our length prefix. 
        mmap[self.current_offset as usize .. message_start as usize]
            .copy_from_slice(&len_bytes);

        // Push our message.
        mmap[message_start as usize .. new_file_end as usize]
            .copy_from_slice(message.as_ref());
        
        match mmap.flush_range(self.current_offset as usize, additional_len as usize) {
            Ok(_) => {
                // Update our current offset counter.
                self.current_offset = new_file_end;
                future::ok(())
            },
            Err(e) => future::err(e.into()),
        }
    }
    fn initial_offset(&self) -> u64 { 
        self.start_offset
    }
    fn record_len(&self) -> u64 { 
        self.current_offset
    }
}

#[cfg(test)]
pub mod test_util {
    use futures::future;

    use super::MessageRecordBackend;

    pub struct DummyBackend{}

    impl MessageRecordBackend for DummyBackend {
        type WriteMsgFuture = futures::future::Ready<Result<(), std::io::Error>>;

        #[allow(unused_variables)]
        fn init<T: AsRef<std::path::Path>>(file_path: T) -> Result<Self, std::io::Error> {
            Ok(DummyBackend{})
        }

        #[allow(unused_variables)]
        fn write_message<T: AsRef<[u8]>>(&mut self, message: T) -> Self::WriteMsgFuture {
            future::ok(())
        }

        fn initial_offset(&self) -> u64 { 0 }
        fn record_len(&self) -> u64 { 0 }
    }
    
    pub struct VecBackend{
        pub inner: Vec<u8>,
    }

    impl MessageRecordBackend for VecBackend {
        type WriteMsgFuture = futures::future::Ready<Result<(), std::io::Error>>;

        #[allow(unused_variables)]
        fn init<T: AsRef<std::path::Path>>(file_path: T) -> Result<Self, std::io::Error> {
            Ok(
                VecBackend{
                    inner: Vec::default(),
                }
            )
        }

        #[allow(unused_variables)]
        fn write_message<T: AsRef<[u8]>>(&mut self, message: T) -> Self::WriteMsgFuture {
            // Length-prefixing. 
            let len_bytes = message.as_ref().len().to_le_bytes();
            self.inner.extend_from_slice(&len_bytes);
            // Write message.
            self.inner.extend_from_slice(message.as_ref());
            future::ok(())
        }

        fn initial_offset(&self) -> u64 { 0 }
        fn record_len(&self) -> u64 {
            self.inner.len() as u64
        }
    }

}