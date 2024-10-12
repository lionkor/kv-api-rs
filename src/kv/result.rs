use std::io;

#[derive(Debug, thiserror::Error)]
pub enum KVError {
    #[error("IO Error: {0}")]
    IO(io::Error),
    #[error("Invalid Data: {0}")]
    InvalidData(String),
}

impl From<io::Error> for KVError {
    fn from(error: io::Error) -> Self {
        KVError::IO(error)
    }
}

pub type KVResult<T> = std::result::Result<T, KVError>;
