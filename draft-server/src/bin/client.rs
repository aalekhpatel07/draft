use tokio::{net::TcpStream, io::AsyncWriteExt};
use draft_core::*;
use rmp_serde::encode;
use std::time::Duration;


#[tokio::main]
pub async fn main() {

    let vote_request: VoteRequest = VoteRequest {
        term: 1,
        candidate_id: 1,
        last_log_index: 1,
        last_log_term: 1,
    };

    loop {
        let mut stream = TcpStream::connect("127.0.0.1:8080").await.expect("Failed to connect");
        let as_vec = encode::to_vec(&vote_request).expect("Failed to encode");
        stream.write_all(&as_vec).await.expect("Failed to write");
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}