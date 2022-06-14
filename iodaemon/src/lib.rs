
use std::path::Path;
use std::fs::File;

trait SequencerConsumer {
    
}

struct BlockingConsumer {
    file: File,
}

impl BlockingConsumer {
    fn new(file: File) -> BlockingConsumer {
        BlockingConsumer {
            file
        }
    }
    
    fn receive() {
        
    }
}
