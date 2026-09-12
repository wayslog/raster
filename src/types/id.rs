//! Use different types for different addresses and identities;Logical validity does not grant memory access permission.
use super::Error;

macro_rules! identifier {
    ($name:ident, $doc:literal) => {
        #[doc = $doc]
        #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(pub [u8; 16]);
    };
}
identifier!(StoreId, "Store identity.");
identifier!(SessionId, "Recoverable session identity.");
identifier!(
    CheckpointToken,
    "checkpoint identity;Yes token It does not mean that it has been persisted."
);
identifier!(FormatId, "Format or layout semantic identifier.");

macro_rules! number {
    ($name:ident, $doc:literal) => {
        #[doc = $doc]
        #[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(pub u64);
    };
}
number!(
    LogAddress,
    "Logical log address,Cannot be used as a memory pointer."
);
number!(
    CacheAddress,
    "Read cache address,Must not be persisted to the primary index."
);
number!(PageId, "Logical page number.");
number!(
    Generation,
    "page,Request the reuse generation of a slot or table."
);
number!(
    Serial,
    "Session operation sequence number provided by the application."
);
number!(
    CheckpointVersion,
    "checkpoint version,and recycling epoch different."
);
number!(EpochVersion, "Access safe recycling versions.");
number!(KeyHash, "Stable key hash.");
number!(IoId, "Device-scoped in-transit request identifier.");
number!(MaintenanceId, "Maintenance task ID within storage scope.");

/// A ticket identity contains the session, storage, and slot generations to prevent
/// misrouting across sessions or late responses.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RequestId {
    pub store: StoreId,
    pub session: SessionId,
    pub slot: u64,
    pub generation: Generation,
}

/// Hash algorithms and their seeds are documented separately from the key-encoded version.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HashDescriptor {
    pub algorithm: FormatId,
    pub seed: Vec<u8>,
}

macro_rules! generated_identifier {
    ($name:ident) => {
        impl $name {
            /// Generated from system random source 128 identity;Return directly on failure,Does not fall back to time or process number.
            /// Uniqueness is a probability guarantee,Recovery and registration still require detection of duplicate identities.
            pub fn generate() -> Result<Self, Error> {
                let value = Self(random_identity()?);
                value.validate()?;
                Ok(value)
            }

            /// All-zero identities are left as invalid values;Existing persistent identities should reuse the original bytes.
            pub fn validate(self) -> Result<(), Error> {
                if self.0 == [0; 16] {
                    Err(Error::InvalidFormat("The identity cannot be all zeros"))
                } else {
                    Ok(())
                }
            }
        }
    };
}
generated_identifier!(StoreId);
generated_identifier!(SessionId);
generated_identifier!(CheckpointToken);

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn random_identity() -> Result<[u8; 16], Error> {
    read_identity(std::fs::File::open("/dev/urandom")?)
}

#[cfg(any(target_os = "linux", target_os = "macos", test))]
fn read_identity(mut source: impl std::io::Read) -> Result<[u8; 16], Error> {
    let mut bytes = [0; 16];
    source.read_exact(&mut bytes)?;
    Ok(bytes)
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn random_identity() -> Result<[u8; 16], Error> {
    Err(Error::InvalidState(
        "Identity generation only supports Linux and macOS",
    ))
}

macro_rules! address {
    ($name:ident) => {
        impl $name {
            /// Invalid sentry;Zero is a valid logical offset.Record existence is checked by the logging layer.
            pub const INVALID: Self = Self(u64::MAX);

            pub fn validate(self) -> Result<(), Error> {
                if self == Self::INVALID {
                    Err(Error::InvalidFormat("Invalid logical address"))
                } else {
                    Ok(())
                }
            }

            /// Address advancement does not allow wraparound or entry into invalid sentinels.
            pub fn checked_add(self, bytes: u64) -> Result<Self, Error> {
                self.validate()?;
                let next = Self(self.0.checked_add(bytes).ok_or(Error::CapacityExceeded)?);
                if next == Self::INVALID {
                    return Err(Error::CapacityExceeded);
                }
                Ok(next)
            }

            /// Page size must be a non-zero power of two;Does not imply that the current page is allocated or still protected.
            pub fn page_offset(self, page_bytes: u64) -> Result<(PageId, u64), Error> {
                self.validate()?;
                validate_page_bytes(page_bytes)?;
                Ok((PageId(self.0 / page_bytes), self.0 % page_bytes))
            }

            pub fn from_page_offset(
                page: PageId,
                offset: u64,
                page_bytes: u64,
            ) -> Result<Self, Error> {
                validate_page_bytes(page_bytes)?;
                if offset >= page_bytes {
                    return Err(Error::InvalidFormat("In-page offset out of bounds"));
                }
                let base = page
                    .0
                    .checked_mul(page_bytes)
                    .ok_or(Error::CapacityExceeded)?;
                Self(base).checked_add(offset)
            }
        }
    };
}
address!(LogAddress);
address!(CacheAddress);

fn validate_page_bytes(bytes: u64) -> Result<(), Error> {
    if !bytes.is_power_of_two() {
        return Err(Error::InvalidConfig {
            field: "page_bytes",
            reason: "Page size must be a non-zero power of two",
        });
    }
    Ok(())
}

impl KeyHash {
    /// high 16 Bits are only used for candidate filtering;Encoded keys must be compared even if full hashes are equal.
    pub const fn tag(self) -> u16 {
        (self.0 >> 48) as u16
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_identity_is_not_returned_when_the_random_source_reading_is_incomplete_or_an_error_is_reported()
     {
        assert!(matches!(read_identity(&[1_u8; 15][..]), Err(Error::Io(_))));
        struct Failed;
        impl std::io::Read for Failed {
            fn read(&mut self, _: &mut [u8]) -> std::io::Result<usize> {
                Err(std::io::Error::other("Test random source failed"))
            }
        }
        assert!(matches!(read_identity(Failed), Err(Error::Io(_))));
        assert_eq!(read_identity(&[42_u8; 16][..]).unwrap(), [42; 16]);
    }
}
