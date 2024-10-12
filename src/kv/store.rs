use std::{
    collections::HashMap,
    io::{self, SeekFrom},
};

use log::debug;
use tokio::{fs::File, io::AsyncSeekExt};

use crate::kv::{entry::KVEntry, result::KVError};

use super::{entry::Entry, memory_noop::MemoryNoOpRWS, result::KVResult};

/// This trait exists to allow for the use of both `File` and `MemoryNoOpRWS` (as well as anything
/// else that implements `AsyncRead`, `AsyncWrite`, `AsyncSeek`, etc. as the backing storage
/// for the KVStore.
pub trait AsyncRWS:
    tokio::io::AsyncRead + tokio::io::AsyncWrite + tokio::io::AsyncSeek + Unpin
{
}

impl<T: tokio::io::AsyncRead + tokio::io::AsyncWrite + tokio::io::AsyncSeek + ?Sized + Unpin>
    AsyncRWS for T
{
}

/// KVStore backed by a file on disk, using `tokio::fs::File` as the backing storage.
pub type FileBackedKVStore = KVStore<File>;
/// In-memory KVStore, using `MemoryNoOpRWS` as the backing storage, which is not persistent.
pub type MemoryBackedKVStore = KVStore<MemoryNoOpRWS>;

/// A key-value store
pub struct KVStore<T>
where
    T: AsyncRWS,
{
    entries: HashMap<String, Entry>,
    stream: Box<T>,
}

impl<T: AsyncRWS> KVStore<T> {
    /// Creates a new KVStore with the provided backing storage. This method will read all entries
    /// from the backing storage and store them in memory, if any exist. If you don't need a
    /// persistent store, consider using `MemoryBackedKVStore` instead.
    pub async fn new(mut backing_stream: Box<T>) -> KVResult<KVStore<T>> {
        let mut entries = HashMap::new();
        backing_stream.seek(SeekFrom::Start(0)).await?;
        loop {
            match KVEntry::read_from_stream(&mut backing_stream).await {
                Ok(entry) => {
                    _ = entries.insert(entry.key.clone(), Entry::from(entry));
                }
                Err(err) => match err {
                    KVError::IO(error) => match error.kind() {
                        io::ErrorKind::UnexpectedEof => {
                            debug!("Reached end of file");
                            break;
                        }
                        _ => {
                            debug!("IO Error of kind: {:?}", error.kind());
                            return Err(KVError::IO(error));
                        }
                    },
                    _ => {
                        debug!("Non-IO error: {:?}", err);
                        return Err(err);
                    }
                },
            }
        }
        debug!("Finished reading all entries");
        Ok(KVStore {
            entries,
            stream: backing_stream,
        })
    }

    /// Get the value as an `Entry` for a given key.
    pub fn get(&self, key: &str) -> Option<&Entry> {
        if let Some(entry) = self.entries.get(key) {
            Some(entry)
        } else {
            None
        }
    }

    /// Set the value for a given key. This will write the entry to the backing storage.
    ///
    /// If the value is large enough, it will be compressed before being written.
    ///
    /// # Errors
    ///
    /// std::io::Error: If there is an error writing to the backing storage.
    ///
    pub async fn set(&mut self, key: &str, value: Entry) -> KVResult<()> {
        let kv_entry = KVEntry {
            key: key.to_owned(),
            value: value.value.clone(),
            mime: value.mime.clone(),
        };
        debug!(
            "Setting entry: key = {:?}, value length = {}, mime = {:?}",
            key,
            value.value.len(),
            value.mime
        );
        // For an in-memory KV store the underlying implementation is a no-op
        // for the following lines which write to the stream.
        if value.value.len() > 1024 {
            debug!("Value length exceeds 1024 bytes, compressing entry");
            kv_entry
                .write_to_stream_compressed(&mut *self.stream)
                .await?;
        } else {
            debug!("Value length is within limit, writing uncompressed entry");
            kv_entry.write_to_stream(&mut *self.stream).await?;
        }
        self.entries.insert(key.to_owned(), value);
        debug!("Entry set successfully: key = {:?}", key);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kv::memory_noop::MemoryNoOpRWS;
    use crate::kv::entry::Entry;
    use crate::kv::result::KVResult;

    #[tokio::test]
    async fn test_kvstore_set_and_get() -> KVResult<()> {
        let memory_stream = Box::new(MemoryNoOpRWS::new());
        let mut kv_store = KVStore::new(memory_stream).await?;

        let key = "test_key";
        let value = Entry {
            value: b"test_value".to_vec(),
            mime: "text/plain".to_string(),
        };

        kv_store.set(key, value.clone()).await?;
        let retrieved_value = kv_store.get(key).unwrap();

        assert_eq!(retrieved_value.value, value.value);
        assert_eq!(retrieved_value.mime, value.mime);

        Ok(())
    }
}
