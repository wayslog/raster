//! Owned canonical key bytes with inline storage for short encodings.
use crate::types::Error;
use std::ops::{Deref, DerefMut};

const INLINE_CAPACITY: usize = 16;

#[derive(Clone)]
pub(crate) enum EncodedKey {
    Inline {
        bytes: [u8; INLINE_CAPACITY],
        length: u8,
    },
    Heap(Vec<u8>),
}

impl EncodedKey {
    pub fn zeroed(length: usize) -> Result<Self, Error> {
        if length <= INLINE_CAPACITY {
            return Ok(Self::Inline {
                bytes: [0; INLINE_CAPACITY],
                length: length as u8,
            });
        }
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(length)
            .map_err(|_| Error::OutOfMemory)?;
        bytes.resize(length, 0);
        Ok(Self::Heap(bytes))
    }
}

impl From<Vec<u8>> for EncodedKey {
    fn from(bytes: Vec<u8>) -> Self {
        if bytes.len() <= INLINE_CAPACITY {
            let mut inline = [0; INLINE_CAPACITY];
            inline[..bytes.len()].copy_from_slice(&bytes);
            Self::Inline {
                bytes: inline,
                length: bytes.len() as u8,
            }
        } else {
            Self::Heap(bytes)
        }
    }
}

impl Deref for EncodedKey {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        match self {
            Self::Inline { bytes, length } => &bytes[..usize::from(*length)],
            Self::Heap(bytes) => bytes,
        }
    }
}

impl DerefMut for EncodedKey {
    fn deref_mut(&mut self) -> &mut [u8] {
        match self {
            Self::Inline { bytes, length } => &mut bytes[..usize::from(*length)],
            Self::Heap(bytes) => bytes,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encoders_receive_exact_zeroed_slices_and_clones_own_their_bytes() {
        for length in [0, 1, 15, 16, 17, 1024] {
            let mut key = EncodedKey::zeroed(length).unwrap();
            assert_eq!(&*key, vec![0; length]);
            key.fill(0xfa);
            let mut cloned = key.clone();
            cloned.fill(0x80);
            assert_eq!(&*key, vec![0xfa; length]);
            assert_eq!(&*cloned, vec![0x80; length]);
            assert_eq!(&*EncodedKey::from(vec![0xfa; length]), &*key);
        }
        assert!(matches!(
            EncodedKey::zeroed(usize::MAX),
            Err(Error::OutOfMemory)
        ));
    }
}
