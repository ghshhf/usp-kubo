//! Data chunking utilities

use bytes::{Bytes, BytesMut};

/// Fixed size chunker
pub struct FixedSizeChunker {
    chunk_size: usize,
}

impl FixedSizeChunker {
    pub fn new(chunk_size: usize) -> Self {
        Self { chunk_size }
    }

    /// Split data into chunks
    pub fn chunk(&self, data: &[u8]) -> Vec<Bytes> {
        data.chunks(self.chunk_size)
            .map(Bytes::copy_from_slice)
            .collect()
    }

    /// Reconstruct data from chunks
    pub fn unchunk(&self, chunks: Vec<Bytes>) -> Bytes {
        let total_len: usize = chunks.iter().map(|c| c.len()).sum();
        let mut result = BytesMut::with_capacity(total_len);
        for chunk in chunks {
            result.extend_from_slice(&chunk);
        }
        result.freeze()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fixed_chunker() {
        let chunker = FixedSizeChunker::new(3);
        let data = b"hello world";
        let chunks = chunker.chunk(data);

        assert_eq!(chunks.len(), 4);
        assert_eq!(&chunks[0][..], b"hel");
        assert_eq!(&chunks[3][..], b"ld");
    }
}
