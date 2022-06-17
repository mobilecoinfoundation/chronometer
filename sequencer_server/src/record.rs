//! A record / history of all messages sent on this consistently-sequenced
//! message bus. The sequencer's message history is split into two memory-mapped
//! ring buffer files, one of which contains the current

use std::{future::Future, path::Path, fs::{File, OpenOptions}, io::{SeekFrom, Seek, Write}};

use futures::future;
use memmap2::{MmapOptions, MmapMut};

pub(crate) type LengthTag = u16; 
pub(crate) const LENGTH_TAG_LEN: usize = std::mem::size_of::<LengthTag>();

/// How many bytes should we add to the file when it's time to grow the file?
const GROW_BY: usize = 65535;

pub trait MessageRecordBackend : Sized {
    type WriteMsgFuture: Future<Output=Result<(), std::io::Error>>; 

    fn init<T: AsRef<Path>>(file_path: T, start_offset: u64) -> Result<Self, std::io::Error>; 
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
        #[cfg(debug_assertions)] { 
            println!("File is currently {} bytes long, assessing if it needs to grow to add {}...", self.file.metadata().unwrap().len(), additional_bytes_len)
        }

        if (self.current_offset + additional_bytes_len) > self.current_file_len { 
            let to_grow = GROW_BY.max(additional_bytes_len as usize);
            let new_len = self.current_file_len + to_grow as u64;


            #[cfg(debug_assertions)] { 
                println!("Growing file to {}...", new_len);
            }

            // Expand file.
            self.file.set_len(new_len)?;
            self.file.seek(SeekFrom::Start(self.current_file_len))?;
            
            // Zeroize the next length tag (null terminate) and don't advance the cursor or current offset (so it can get overwritten)
            let zeroes = [0u8; LENGTH_TAG_LEN];
            self.file.write_all(&zeroes)?;
            self.file.seek(SeekFrom::Start(self.current_file_len))?;
                
            self.current_file_len = new_len;
            self.mmap = { 
                
                let mut options = MmapOptions::default();
                // Append - start at the end of the file. 
                options.offset(self.current_offset);
                options.len(new_len as usize - self.current_offset as usize);

                #[cfg(debug_assertions)] { 
                    println!("Memory map will start at {} and be {} bytes long", self.current_offset, to_grow);
                }

                // There is, apparently, no such thing as a "safe" memory map
                // since, at an OS level, not much safety is built in -
                // other processes can change them out from under you.
                unsafe {
                    options.map_mut(&self.file)?
                }
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
    type WriteMsgFuture = futures::future::Ready<Result<(), std::io::Error>>;

    fn init<T: AsRef<Path>>(file_path: T, start_offset: u64) -> Result<Self, std::io::Error> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .append(true)
            .create(true)
            .open(file_path)?;
        
        let file_len = file.seek(SeekFrom::End(0))?;

        let start_offset = if start_offset > file_len { 
            println!("WARNING: Attempting to start at an offset which the message bus has not reached yet.");
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

        Ok(
            Self {
                file,
                start_offset,
                current_offset: start_offset,
                current_file_len: file_len,
                mmap_cursor: 0,
                mmap,
            }
        )
    }

    #[inline]
    fn write_message<T: AsRef<[u8]>>(&mut self, message: T) -> Self::WriteMsgFuture {
        // Length of message payload to append.
        let message_len = message.as_ref().len() as usize; 
        assert!(message_len < LengthTag::MAX as usize);
        // Total new bytes to append.
        let additional_len = (LENGTH_TAG_LEN + message_len) as usize;
        // End of length prefix, start of message payload.
        //let message_start = self.current_offset + LENGTH_PREFIX_LEN as u64;
        // Length prefix bytes.
        // TODO: Discuss with team - are we sure we want little-endian? 
        let len_bytes = (message.as_ref().len() as LengthTag).to_le_bytes();
        let full_len = LENGTH_TAG_LEN + message_len as usize;
        // How long will the file need to be to contain this new information?
        let new_record_end = self.current_offset + additional_len as u64;

        // Grow the file and associated memmap
        if let Err(e) = self.grow_if_needed(additional_len as u64) { 
            return future::err(e);
        }

        /*#[cfg(unix)]
        { 
            // This is a Unix-only feature so there is no 
            // perf hint on, for example, Azure.
            if let Err(e) = mmap.advise(memmap2::Advice::Sequential) {
                return future::err(e);
            }
        }*/
 
        // Current range of the memory map right now appears to be offset..len - this affects how the slice works. 
        //mmap[self.current_offset as usize .. message_start as usize]
        //    .copy_from_slice(&len_bytes);
        //mmap[message_start as usize .. new_file_end as usize]
        //    .copy_from_slice(message.as_ref());

        // Push our length prefix.
        self.mmap[self.mmap_cursor .. self.mmap_cursor+LENGTH_TAG_LEN]
            .copy_from_slice(&len_bytes);
        // Push our message.
        self.mmap[self.mmap_cursor+LENGTH_TAG_LEN .. self.mmap_cursor+full_len]
            .copy_from_slice(message.as_ref());

        let mut written_len = full_len;

        // Zeroize the next length tag, and don't advance the cursor or current offset (so it can get overwritten)
        if (self.current_offset + full_len as u64 + LENGTH_TAG_LEN as u64) > self.current_file_len { 
            let zeroes = [0u8; LENGTH_TAG_LEN];
            self.mmap[self.mmap_cursor+full_len .. self.mmap_cursor+full_len+LENGTH_TAG_LEN]
                .copy_from_slice(&zeroes);
            written_len += LENGTH_TAG_LEN;
        }
        
        match self.mmap.flush_range(self.mmap_cursor, written_len) {
            Ok(_) => {
                // Update our current offset counter.
                self.current_offset = new_record_end;
                self.mmap_cursor += written_len;
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

    use super::{MessageRecordBackend, LengthTag};

    pub struct DummyBackend{}

    impl MessageRecordBackend for DummyBackend {
        type WriteMsgFuture = futures::future::Ready<Result<(), std::io::Error>>;

        #[allow(unused_variables)]
        fn init<T: AsRef<std::path::Path>>(file_path: T, start_offset: u64) -> Result<Self, std::io::Error> {
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
        fn init<T: AsRef<std::path::Path>>(file_path: T, start_offset: u64) -> Result<Self, std::io::Error> {
            Ok(
                VecBackend{
                    inner: Vec::default(),
                }
            )
        }

        #[allow(unused_variables)]
        fn write_message<T: AsRef<[u8]>>(&mut self, message: T) -> Self::WriteMsgFuture {
            // Length-prefixing. 
            let len_bytes = (message.as_ref().len() as LengthTag).to_le_bytes();
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