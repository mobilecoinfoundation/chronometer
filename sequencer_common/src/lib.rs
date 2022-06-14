#![no_std]

extern crate alloc;
use core::pin::Pin;

use alloc::vec::Vec;

use bytecheck::CheckBytes;
use rkyv::{Archive, Deserialize, Serialize};

pub type AppId = u16;
pub type EpochId = u16;
pub type InstanceId = u16;
pub type ClusterId = u16;

/// Messages going into the sequencer server and 
/// being sent out by the sequencer
/// server over the bus are the same structure, 
/// to make 0-copy behavior possible.
#[derive(Archive, Deserialize, Serialize, Debug, PartialEq, Eq)]
#[archive(compare(PartialEq))]
#[archive_attr(derive(CheckBytes, Debug))]
pub struct SequencerMessage {
    /// Number of bytes from the start of this epoch.
    pub offset: u64,
    /// On the way into the bus, from senders: This is the per-app sequence
    /// number used to deduplicate. On the bus, sent by the sequencer:
    /// Global canonical sequence number for all bus participants
    pub sequence_number: u64,
    pub epoch: EpochId,
    pub instance_id: InstanceId,
    pub app_id: AppId,            // Potentially replace with non-exhaustive enum
    pub app_sequence_number: u64, // Potentially replace with non-exhaustive enum
    pub cluster_id: ClusterId,    //Known in other contexts as an active-active ID
    pub payload: Vec<u8>,
}

impl SequencerMessage {
    pub fn new(
        app_id: AppId,
        instance_id: InstanceId,
        cluster_id: ClusterId,
        app_sequence_number: u64,
        payload: Vec<u8>,
    ) -> Self {
        SequencerMessage {
            offset: 0,
            sequence_number: 0,
            epoch: 0,
            app_id,
            instance_id,
            cluster_id,
            app_sequence_number,
            payload,
        }
    }
}

impl ArchivedSequencerMessage {
    #[inline(always)]
    // This is the pin projection from SequencerMessage -> sequence_number
    pub fn modify_sequence_number(self: Pin<&mut Self>, value: u64) -> Pin<&mut Self> {
        // Sequence number is not a reference type and does not contain any reference
        // types, so this unsafe block *should* be good here.
        unsafe {
            let reference = self.get_unchecked_mut();
            reference.sequence_number = value;
            Pin::new_unchecked(reference)
        }
    }
}
