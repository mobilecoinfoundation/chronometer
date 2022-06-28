use nix::sys::mman::{mmap, mremap, MRemapFlags, MapFlags, ProtFlags};
use rkyv::check_archived_root;
use std::{ffi::c_void, fs::File, os::unix::io::AsRawFd, path::Path, slice};

use sequencer_common::{ArchivedSequencerMessage, SequencerMessage};

#[derive(Debug, Clone, Copy)]
pub enum IODaemonError {
    InsufficientData,
}

#[derive(Debug)]
pub struct BlockingConsumer<'a> {
    file: File,
    mapping: *mut u8,
    file_length: usize,
    mapping_length: usize,
    cursor: usize,
    phantom: std::marker::PhantomData<&'a u8>,
}

const PAGE_LEN: usize = 4096;
const CHUNK_SIZE: usize = PAGE_LEN * 16;

fn to_nearest(value: usize, increment: usize) -> usize {
    (value + (increment - 1) / increment) * increment
}

impl<'a> BlockingConsumer<'a> {
    pub fn new(path: impl AsRef<Path>) -> BlockingConsumer<'a> {
        let file = File::open(path).expect("couldn't open file");
        let file_length = file.metadata().expect("couldn't get file metadata").len() as usize;
        let mapping_length = if file_length == 0 {
            CHUNK_SIZE
        } else {
            to_nearest(file_length, CHUNK_SIZE)
        }; // round to nearest CHUNK_SIZE multiple
        let mapping = unsafe {
            mmap(
                std::ptr::null_mut(),
                mapping_length,
                ProtFlags::PROT_READ,
                MapFlags::MAP_SHARED,
                file.as_raw_fd(),
                0,
            )
        }
        .expect("couldn't map file") as *mut u8;
        BlockingConsumer {
            file,
            mapping,
            file_length,
            mapping_length,
            cursor: 0,
            phantom: std::marker::PhantomData::default(),
        }
    }

    fn remap(&mut self) {
        let file_length = self
            .file
            .metadata()
            .expect("couldn't get file metadata")
            .len() as usize;
        let mapping_length = to_nearest(file_length, CHUNK_SIZE); // round to nearest CHUNK_SIZE multiple

        let mapping = unsafe {
            mremap(
                self.mapping as *mut c_void,
                self.mapping_length,
                mapping_length,
                MRemapFlags::MREMAP_MAYMOVE,
                None,
            )
        }
        .expect("couldn't map file") as *mut u8;
        self.mapping = mapping;
        self.file_length = file_length;
        self.mapping_length = mapping_length;
    }

    // same as read but doesn't update cursor
    fn peek(&mut self, length: usize) -> Result<&[u8], IODaemonError> {
        if self.cursor + length > self.file_length {
            // update file length
            self.file_length = self
                .file
                .metadata()
                .expect("couldn't get file metadata")
                .len() as usize;
            // see if it's enough now
            if self.cursor + length > self.file_length {
                return Err(IODaemonError::InsufficientData);
            }
        }

        if self.cursor + length > self.mapping_length {
            self.remap();

            if self.cursor + length > self.mapping_length {
                return Err(IODaemonError::InsufficientData);
            }
        }

        let u8_mapping = unsafe { slice::from_raw_parts(self.mapping, self.file_length) };
        Ok(&u8_mapping[self.cursor..self.cursor + length])
    }

    // this function doesn't call peek because of rustc#54663
    fn read(&mut self, length: usize) -> Result<&[u8], IODaemonError> {
        if self.cursor + length > self.file_length {
            // update file length
            self.file_length = self
                .file
                .metadata()
                .expect("couldn't get file metadata")
                .len() as usize;
            // see if it's enough now
            if self.cursor + length > self.file_length {
                return Err(IODaemonError::InsufficientData);
            }
        }

        if self.cursor + length > self.mapping_length {
            self.remap();

            if self.cursor + length > self.mapping_length {
                return Err(IODaemonError::InsufficientData);
            }
        }

        let u8_mapping = unsafe { slice::from_raw_parts(self.mapping, self.file_length) };
        let result = &u8_mapping[self.cursor..self.cursor + length];
        self.cursor += length;
        Ok(result)
    }

    pub fn read_message(&mut self) -> Result<&ArchivedSequencerMessage, IODaemonError> {
        // read length
        // we have to do this instead of just handling read's return because of
        // rustc#54663
        let len_slice = self.peek(8)?;
        let len = u64::from_be_bytes(len_slice.try_into().unwrap()) as usize;

        // read message
        // again, reading length twice because of the rustc bug
        self.read(len + 8)
            .map(|x| check_archived_root::<SequencerMessage>(&x[8..]).expect("invalid message"))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use byteorder::{NetworkEndian, WriteBytesExt};
    use rand::random;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_readwrite() {
        // open temp file
        let test_file = NamedTempFile::new().unwrap();
        let mut write_end = test_file.reopen().unwrap();
        let mut reader = BlockingConsumer::new(test_file.path());

        let mut messages = vec![];

        for _ in 0..10 {
            // generate random message
            let message = SequencerMessage::new(
                random(),
                random(),
                random(),
                random(),
                (0..CHUNK_SIZE).map(|_| random()).collect(),
            );
            messages.push(message);
        }

        for message in messages {
            // archive message
            let bytes = rkyv::to_bytes::<_, 256>(&message).unwrap();

            assert!(reader.read_message().is_err());

            // write length as bigendian
            write_end
                .write_u64::<NetworkEndian>(bytes.len() as u64)
                .unwrap();
            write_end.sync_all().unwrap();

            assert!(reader.read_message().is_err());

            // write archive
            write_end.write(&bytes).unwrap();
            write_end.sync_all().unwrap();

            // read from BlockingConsumer
            let read_message = reader.read_message().unwrap();

            // check for equality
            assert!(*read_message == message);

            assert!(reader.read_message().is_err());
        }
    }

    #[test]
    fn test_partial() {
        // open temp file
        let test_file = NamedTempFile::new().unwrap();
        let mut write_end = test_file.reopen().unwrap();
        let mut reader = BlockingConsumer::new(test_file.path());

        let mut messages = vec![];

        for _ in 0..10 {
            // generate random message
            let message = SequencerMessage::new(
                random(),
                random(),
                random(),
                random(),
                (0..CHUNK_SIZE).map(|_| random()).collect(),
            );
            messages.push(message);
        }

        for message in messages {
            // archive message
            let bytes = rkyv::to_bytes::<_, 256>(&message).unwrap();

            assert!(reader.read_message().is_err());

            // write length as bigendian
            write_end
                .write_u64::<NetworkEndian>(bytes.len() as u64)
                .unwrap();
            write_end.sync_all().unwrap();

            assert!(reader.read_message().is_err());

            // write half the archive
            write_end.write(&bytes[..(CHUNK_SIZE / 2)]).unwrap();
            write_end.sync_all().unwrap();

            assert!(reader.read_message().is_err());

            // write other half
            write_end.write(&bytes[(CHUNK_SIZE / 2)..]).unwrap();
            write_end.sync_all().unwrap();

            // read from BlockingConsumer
            let read_message = reader.read_message().unwrap();

            // check for equality
            assert!(*read_message == message);

            assert!(reader.read_message().is_err());
        }
    }

    #[test]
    fn test_buffered() {
        // open temp file
        let test_file = NamedTempFile::new().unwrap();
        let mut write_end = test_file.reopen().unwrap();
        let mut reader = BlockingConsumer::new(test_file.path());

        let mut messages = vec![];

        for _ in 0..10 {
            // generate random message
            let message = SequencerMessage::new(
                random(),
                random(),
                random(),
                random(),
                (0..CHUNK_SIZE).map(|_| random()).collect(),
            );
            messages.push(message);
        }

        for message in &messages {
            // archive message
            let bytes = rkyv::to_bytes::<_, 256>(message).unwrap();

            // write length as bigendian
            write_end
                .write_u64::<NetworkEndian>(bytes.len() as u64)
                .unwrap();

            // write archive
            write_end.write(&bytes).unwrap();
        }

        write_end.sync_all().unwrap();

        for message in messages {
            // read from BlockingConsumer
            let read_message = reader.read_message().unwrap();

            // check for equality
            assert!(*read_message == message);
        }

        assert!(reader.read_message().is_err());
    }
}
