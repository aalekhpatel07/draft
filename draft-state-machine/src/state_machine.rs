use bytes::Bytes;
use async_trait::async_trait;

#[async_trait]
pub trait RaftStateMachine: Default 
{
    type Err: std::fmt::Debug + Send + Sync + 'static;
    async fn apply(&self, entry: Bytes) -> core::result::Result<Bytes, Self::Err>;
}

#[async_trait]
pub trait StateMachine: Default {
    type Request: From<Bytes> + Send + Sync + 'static;
    type Response: Into<Bytes> + Send + Sync + 'static;
    type Err: std::fmt::Debug + Send + Sync + 'static;

    fn new() -> Self;
    async fn _apply(&self, entry: Self::Request) -> core::result::Result<Self::Response, Self::Err>;
}

#[async_trait]
impl<Db> RaftStateMachine for Db
where
    Db: StateMachine + Sync + Send 
{
    type Err = Db::Err;

    async fn apply(&self, entry: Bytes) -> core::result::Result<Bytes, Self::Err> {

        self
        ._apply(entry.into())
        .await
        .map_err(|x| x.into())
        .map(|resp| resp.into())
    }
}