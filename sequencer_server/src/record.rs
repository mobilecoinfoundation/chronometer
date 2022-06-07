//! A record / history of all messages sent on this consistently-sequenced
//! message bus. The sequencer's message history is split into two memory-mapped
//! ring buffer files, one of which contains the current
