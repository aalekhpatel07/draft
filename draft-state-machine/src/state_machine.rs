use bytes::Bytes;
use async_trait::async_trait;
use tokio::sync::mpsc;


pub type CommandID = uuid::Uuid;

/// The messages that Raft will send to the state machine
/// and therefore it should know how to handle them.
#[derive(Debug)]
pub enum IncomingMessage {
    ApplyCommand((CommandID, Bytes))
}

/// The messages that the state machine will send out to Raft
/// and therefore Raft should know how to handle these.
#[derive(Debug)]
pub enum OutgoingMessage {
    CommandApplied((CommandID, Bytes)),
    FailedToApplyCommand((CommandID, Bytes)),
    RaftError((CommandID, Bytes))
}

#[derive(Debug)]
/// The communication channel between Raft and the State Machine.
pub struct RaftChannels {
    pub rx: mpsc::UnboundedReceiver<(mpsc::UnboundedSender<OutgoingMessage>, IncomingMessage)>,
}

#[async_trait]
pub trait StateMachine {
    type Request: From<Bytes> + Send + Sync + 'static;
    type Response: Into<Bytes> + Send + Sync + 'static;
    type Err: Into<Bytes> + std::fmt::Debug + Send + Sync + 'static;

    async fn apply(&self, entry: Self::Request) -> Result<Self::Response, Self::Err>;
}

#[async_trait]
pub trait RaftStateMachine
{
    type Err: Into<Bytes> + std::fmt::Debug + Send + Sync + 'static;
    async fn run(&self, channels: RaftChannels) -> Result<(), Self::Err>;
}

#[async_trait]
impl<Db> RaftStateMachine for Db
where
    Db: StateMachine + Sync
{
    type Err = Db::Err;

    async fn run(&self, channels: RaftChannels) -> Result<(), Self::Err> {
        let mut rx = channels.rx;

        while let Some((tx, msg)) = rx.recv().await {
            match msg {
                IncomingMessage::ApplyCommand((cmd_id, cmd)) => {
                    match self.apply(cmd.into()).await {
                        Ok(response) => {
                            tx.send(OutgoingMessage::CommandApplied((cmd_id, response.into()))).unwrap();
                        },
                        Err(err) => {
                            tx.send(OutgoingMessage::FailedToApplyCommand((cmd_id, err.into()))).unwrap();
                        }
                    }
                }
            }
        }
        Ok(())
    }
}