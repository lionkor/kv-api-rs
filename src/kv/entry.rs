use async_compression::tokio::bufread::ZstdDecoder;
use async_compression::tokio::write::ZstdEncoder;
use std::{io::Cursor, ops::BitAnd};
use tokio::io::{self, AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt, AsyncWriteExt};

use super::result::{KVError, KVResult};

/// Internal representation of a key-value store entry.
pub(crate) struct KVEntry {
    pub(crate) key: String,
    pub(crate) value: Vec<u8>,
    pub(crate) mime: String,
}

/// Flags stored before each entry, indicating different properties of the entry,
/// such as whether the value is compressed.
#[repr(u8)]
enum Flags {
    None = 0,
    ZstdCompressed = 0b10000000,
}

impl KVEntry {
    /// Creates a new KVEntry with the given key, value, and MIME type.
    pub fn new(key: String, value: Vec<u8>, mime: String) -> Self {
        Self { key, value, mime }
    }

    /// Writes the KVEntry to the provided stream, without compressing the value.
    ///
    /// This method serializes the key, value, and MIME type of the KVEntry
    /// and writes them to the given stream.
    pub(crate) async fn write_to_stream(
        &self,
        mut stream: impl AsyncWriteExt + Unpin,
    ) -> Result<(), io::Error> {
        stream.write_all(&(Flags::None as u8).to_le_bytes()).await?;
        stream
            .write_all(&(self.key.len() as u16).to_le_bytes())
            .await?;
        stream.write_all(self.key.as_bytes()).await?;
        stream
            .write_all(&(self.value.len() as u32).to_le_bytes())
            .await?;
        stream.write_all(&self.value).await?;
        stream
            .write_all(&(self.mime.len() as u16).to_le_bytes())
            .await?;
        stream.write_all(self.mime.as_bytes()).await?;
        Ok(())
    }

    /// Writes the KVEntry to the provided stream, compressing the value with Zstd.
    ///
    /// This method serializes the key, value, and MIME type of the KVEntry
    /// and writes them to the given stream.
    pub(crate) async fn write_to_stream_compressed(
        &self,
        mut stream: impl AsyncWriteExt + AsyncSeek + Unpin,
    ) -> Result<(), io::Error> {
        stream
            .write_all(&(Flags::ZstdCompressed as u8).to_le_bytes())
            .await?;
        let pos = stream.stream_position().await?;
        // this is a placeholder
        stream.write_all(&0u32.to_le_bytes()).await?;
        let start = stream.stream_position().await?;
        let mut encoder = ZstdEncoder::new(stream);
        encoder
            .write_all(&(self.key.len() as u16).to_le_bytes())
            .await?;
        encoder.write_all(self.key.as_bytes()).await?;
        encoder
            .write_all(&(self.value.len() as u32).to_le_bytes())
            .await?;
        encoder.write_all(&self.value).await?;
        encoder
            .write_all(&(self.mime.len() as u16).to_le_bytes())
            .await?;
        encoder.write_all(self.mime.as_bytes()).await?;
        encoder.flush().await?;
        let mut stream = encoder.into_inner();
        let end = stream.stream_position().await?;
        stream.seek(io::SeekFrom::Start(pos)).await?;
        stream
            .write_all(&((end - start) as u32).to_le_bytes())
            .await?;
        stream.seek(io::SeekFrom::Start(end)).await?;
        Ok(())
    }

    /// Reads the key, value, and MIME type of the KVEntry from the given stream.
    /// The stream is assumed to be at the start of the KVEntry, and the flags byte
    /// has already been read.
    pub(crate) async fn read_from_stream_impl<T: AsyncRead + Unpin>(
        mut stream: T,
    ) -> KVResult<Self> {
        let mut key_len_bytes = [0u8; 2];
        stream.read_exact(&mut key_len_bytes).await?;
        let key_len = u16::from_le_bytes(key_len_bytes) as usize;
        let mut key = vec![0u8; key_len];
        stream.read_exact(&mut key).await?;

        let mut value_len_bytes = [0u8; 4];
        stream.read_exact(&mut value_len_bytes).await?;
        let value_len = u32::from_le_bytes(value_len_bytes) as usize;
        let mut value = vec![0u8; value_len];
        stream.read_exact(&mut value).await?;

        let mut mime_len_bytes = [0u8; 2];
        stream.read_exact(&mut mime_len_bytes).await?;
        let mime_len = u16::from_le_bytes(mime_len_bytes) as usize;
        let mut mime = vec![0u8; mime_len];
        stream.read_exact(&mut mime).await?;

        Ok(Self {
            key: String::from_utf8(key)
                .map_err(|_| KVError::InvalidData("Invalid UTF-8 in key".to_string()))?,
            value,
            mime: String::from_utf8(mime)
                .map_err(|_| KVError::InvalidData("Invalid UTF-8 in MIME".to_string()))?,
        })
    }

    /// Reads a KVEntry from the given stream, decompressing the value with Zstd if necessary.
    /// Delegates to `read_from_stream_impl` to read the key, value, and MIME type after
    /// decompression (if applicable).
    pub(crate) async fn read_from_stream(mut stream: impl AsyncReadExt + Unpin) -> KVResult<Self> {
        let flags = stream.read_u8().await?;
        let compressed = flags.bitand(Flags::ZstdCompressed as u8) != 0;
        if compressed {
            let in_len = stream.read_u32_le().await? as usize;
            let mut in_stream = vec![0u8; in_len];
            stream.read_exact(&mut in_stream).await?;

            let mut decomp_stream = Vec::new();
            tokio::io::copy(&mut &in_stream[..], &mut decomp_stream).await?;

            let entry = Self::read_from_stream_impl(&mut &decomp_stream[..]).await?;
            Ok(entry)
        } else {
            Self::read_from_stream_impl(stream).await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::BufWriter;
    use tokio::io::BufReader;

    #[tokio::test]
    async fn test_write_and_read_entry() -> KVResult<()> {
        let entry = KVEntry::new(
            "test_key".to_string(),
            b"test_value".to_vec(),
            "text/plain".to_string(),
        );

        let mut buffer = Vec::new();
        {
            let mut writer = BufWriter::new(&mut buffer);
            entry.write_to_stream(&mut writer).await?;
            writer.flush().await?;
        }

        let mut reader = BufReader::new(&buffer[..]);
        let read_entry = KVEntry::read_from_stream(&mut reader).await?;

        assert_eq!(entry.key, read_entry.key);
        assert_eq!(entry.value, read_entry.value);
        assert_eq!(entry.mime, read_entry.mime);

        Ok(())
    }

    #[tokio::test]
    async fn test_write_and_read_compressed_entry() -> KVResult<()> {
        use rand::Rng;

        let mut rng = rand::thread_rng();
        // enough bytes to cause the compression to kick in
        let random_bytes: Vec<u8> = (0..4096).map(|_| rng.gen()).collect();

        let entry = KVEntry::new(
            "test_key".to_string(),
            random_bytes,
            "text/plain".to_string(),
        );
        let mut buffer = Vec::new();
        {
            let mut cursor = Cursor::new(&mut buffer);
            entry.write_to_stream(&mut cursor).await?;
            cursor.flush().await?;
        }

        let mut reader = BufReader::new(&buffer[..]);
        let read_entry = KVEntry::read_from_stream(&mut reader).await?;

        assert_eq!(entry.key, read_entry.key);
        assert_eq!(entry.value, read_entry.value);
        assert_eq!(entry.mime, read_entry.mime);

        Ok(())
    }
}

/// An abstract value + mime type pair.
///
/// This struct is used to represent the value and MIME type of a KVEntry,
/// which it gets converted to. It is used to abstract away the underlying
/// storage mechanism of the KVEntry.
#[derive(Clone, Debug)]
pub struct Entry {
    pub value: Vec<u8>,
    pub mime: String,
}
impl Entry {
    pub fn new(value: Vec<u8>, mime: String) -> Self {
        Self { value, mime }
    }
}

impl From<KVEntry> for Entry {
    fn from(value: KVEntry) -> Self {
        Self {
            value: value.value,
            mime: value.mime,
        }
    }
}
