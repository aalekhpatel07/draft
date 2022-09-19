use tokio::sync::mpsc::UnboundedSender;
use crate::AppendEntriesResponse;
use crate::RaftNode;
use crate::Storage;


pub fn handle_append_entries_response<S>(
    _receiver_node: &RaftNode<S>,
    _response: AppendEntriesResponse,
    _peer_id: usize,
    _append_entries_outbound_request_tx: UnboundedSender<()>,

) -> color_eyre::Result<()> 
where
    S: Storage + Default
{

    Ok(())
}

#[cfg(test)]
pub mod tests {
    // TODO: Write a macro and tests for handling an append entries response.
}