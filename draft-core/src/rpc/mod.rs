mod request_vote;
mod append_entries;

pub use request_vote::*;
pub use append_entries::*;

use tracing::instrument;
use crate::node::RaftNode;


/// Whoever implements this trait must propagate any errors that occur during the RPC call.
/// These methods don't mutate state.
pub trait RaftRPC {
    fn handle_request_vote(
        &self,
        request: VoteRequest,
    ) -> Result<VoteResponse, RequestVoteRPCError>;
    fn handle_append_entries(
        &self,
        request: AppendEntriesRequest,
    ) -> color_eyre::Result<AppendEntriesResponse>;
}


impl RaftRPC for RaftNode {
    #[instrument(skip(self), target = "rpc::AppendEntries")]
    fn handle_append_entries(
        &self,
        request: AppendEntriesRequest,
    ) -> color_eyre::Result<AppendEntriesResponse> {
        unimplemented!()
    }
    /// Given a vote request RPC, process the request without making any modifications to the state
    /// as described in Section 5.4.1 and Figure 3.
    #[instrument(skip(self), target = "rpc::RequestVote")]
    fn handle_request_vote(
        &self,
        request: VoteRequest,
    ) -> Result<VoteResponse, RequestVoteRPCError> {
        handle_request_vote(&self, request)
    }
}