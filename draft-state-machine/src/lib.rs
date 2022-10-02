mod state_machine;

#[cfg(feature = "draft-mini-redis")]
pub use draft_mini_redis;

use draft_mini_redis::{Command, Connection};
use std::io::Cursor;
use tracing::{debug, error, instrument};
pub use state_machine::*;
use async_trait::async_trait;


#[derive(Debug)]
pub struct MiniRedis {
    db_holder: draft_mini_redis::DbDropGuard,
}

impl MiniRedis {
    pub fn new() -> Self {
        Self { db_holder: draft_mini_redis::DbDropGuard::new() }
    }
}

impl Default for MiniRedis {
    fn default() -> Self {
        Self::new()
    }
}


#[async_trait]
impl RaftStateMachine for MiniRedis {
    type Err = draft_mini_redis::Error;
    
    #[instrument(skip(self))]
    async fn apply(&self, entry: bytes::Bytes) -> core::result::Result<bytes::Bytes, Self::Err> {

        let mut cursor = Cursor::new(&entry[..]);
        let frame = draft_mini_redis::frame::Frame::parse(&mut cursor)?;
        let cmd = Command::from_frame(frame)?;

        debug!(?cmd);

        let holder_cp = self.db_holder.db.clone();
        let (_shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel(1);

        let cursor = Cursor::new(vec![]);

        let mut conn = Connection::new(cursor);
        let mut shutdown = draft_mini_redis::Shutdown { shutdown: false, notify: shutdown_rx};

        if let Err(err) = cmd.apply(&holder_cp, &mut conn, &mut shutdown).await
        {
            error!("Some error occurred when trying to write: {}", err);
        }
        
        let written_data = conn.stream.buffer().to_owned();
        Ok(bytes::Bytes::from(written_data))
    }
}


#[cfg(test)]
pub mod tests {
    use super::*;
    use bytes::Bytes;

    #[cfg(all(not(tarpaulin), not(feature = "flush")))]
    #[tokio::test]
    /**
     * Use [RESP protocol](https://redis.io/docs/reference/protocol-spec/) for communication.
     */
    async fn apply_command_in_resp_format() {
        let redis = MiniRedis::default();
        let command = Bytes::from("*1\r\n$4\r\nPING\r\n");
        let resp = redis.apply(command).await;
        assert_eq!(resp.unwrap(), bytes::Bytes::from("+PONG\r\n"));

        // Send a `GET key` command in the RESP format.
        let command = Bytes::from("*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n");
        let resp = redis.apply(command).await;
        // Since the key doesn't yet exist, we get a Null value back.
        assert_eq!(resp.unwrap(), bytes::Bytes::from("$-1\r\n"));
        

        // Send a `SET key dog` command in the RESP format.
        let command = Bytes::from("*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$3\r\ndog\r\n");
        let resp = redis.apply(command).await;
        // We get an OK response back.
        assert_eq!(resp.unwrap(), bytes::Bytes::from("+OK\r\n"));
    }
}