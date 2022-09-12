mod utils;
pub use utils::*;

use draft_core::{AppendEntriesRequest, VoteRequest};
use tokio::sync::mpsc;

#[derive(Debug, Clone)]
pub struct RPCSenderChannels {
    pub request_vote: mpsc::UnboundedSender<VoteRequest>,
    pub append_entries: mpsc::UnboundedSender<AppendEntriesRequest>,
}

pub fn setup_logging() {

}