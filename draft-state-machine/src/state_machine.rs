use bytes::Bytes;



pub trait RaftStateMachine 
{
    type Err;
    fn apply(&self, entry: Bytes) -> core::result::Result<Bytes, Self::Err>;
}

pub trait StateMachine {
    type Request: From<Bytes>;
    type Response: Into<Bytes>;
    type Err;

    fn _apply(&self, entry: Self::Request) -> core::result::Result<Self::Response, Self::Err>;
}


impl<Db> RaftStateMachine for Db
where
    Db: StateMachine
{
    type Err = Db::Err;

    fn apply(&self, entry: Bytes) -> core::result::Result<Bytes, Self::Err> {
        self._apply(entry.into()).map(|x| x.into())
    }
}