#![no_std]

extern crate alloc;
use alloc::vec::Vec;

use bytes::{BytesMut, BufMut};
use rkyv::{Archive, Deserialize, Serialize, AlignedVec, Archived};
use bytecheck::CheckBytes;

#[derive(Archive, Deserialize, Serialize, Debug, PartialEq)]
#[archive(compare(PartialEq))]
#[archive_attr(derive(CheckBytes, Debug))]
pub struct UnsequencedInput {
    app_id: u32,
    instance_id: u16,
    cluster_id: u16,
    sequence_number: u64,
    payload: Vec<u8>
}

impl UnsequencedInput {
    pub fn new(app_id: u32, instance_id: u16, cluster_id: u16, sequence_number: u64, payload: Vec<u8>) -> Self {
        UnsequencedInput {
            app_id,
            instance_id,
            cluster_id,
            sequence_number,
            payload
        }
    }
}

fn main() {
}
