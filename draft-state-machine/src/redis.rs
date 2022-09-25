pub use hashbrown::HashMap;
use crate::StateMachine;
use serde_json::Value;
use std::{sync::{Arc, Mutex}, str::FromStr};
use bytes::{Bytes, BytesMut};
use serde::{Serialize, Deserialize, Serializer};
use thiserror::Error;


#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Get {
    pub key: String
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Put {
    pub key: String,
    pub value: Value
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Delete {
    pub key: String
}


#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag="kind")]
pub enum Command {
    GET(Get),
    PUT(Put),
    DELETE(Delete),
}

impl From<Bytes> for Command {
    fn from(data: Bytes) -> Self {
        serde_json::from_slice(&data).unwrap()
    }
}
impl From<Command> for Bytes {
    fn from(cmd: Command) -> Self {
        Bytes::from(serde_json::to_vec(&cmd).unwrap())
    }
}

impl ToString for Command {
    fn to_string(&self) -> String {
        match self {
            Command::DELETE(cmd) => {
                format!("DELETE {}", cmd.key)
            },
            Command::PUT(cmd) => {
                format!("PUT {} {}", cmd.key, cmd.value)
            },
            Command::GET(cmd) => {
                format!("GET {}", cmd.key)
            },
        }
    }
}

impl FromStr for Command {
    type Err = std::io::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut iter = s.split_whitespace();
        match iter.next() {
            Some(kind) if kind == "DELETE" => {
                if let Some(key) = iter.next() {
                    Ok(Command::DELETE(Delete { key: key.to_string() }))
                }
                else {
                    Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "DELETE command requires a key."))
                }

            },
            Some(kind) if kind == "PUT" => {
                let mut key: Option<String> = None;
                let mut value: Option<Value> = None;

                if let Some(_key) = iter.next() {
                    key = Some(_key.to_owned());
                }
                else {
                    return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "PUT command requires a key."))
                }

                if let Some(_value ) = iter.next() {
                    value = Some(Value::from_str(_value)?);
                }
                else {
                    return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "PUT command requires a value for the provided key."))
                }

                return Ok(Command::PUT(Put { key: key.unwrap(), value: value.unwrap() }))

            },
            Some(kind) if kind == "GET" => {
                if let Some(key) = iter.next() {
                    Ok(Command::GET(Get { key: key.to_string() }))
                }
                else {
                    Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "GET command requires a key."))
                }
            },
            Some(_) => {
                Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "Unknown command found. A command must be one of GET, PUT, or DELETE."))
            },
            None => {
                Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "No command specified."))
            }
        }
    }
}


#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum RedisResponse<V> {
    Success(V),
    Failed(RedisError)
}

impl<V> From<RedisResponse<V>> for Bytes 
where
    V: Serialize,
{
    fn from(response: RedisResponse<V>) -> Self {
        Bytes::from(serde_json::to_vec(&response).unwrap())
    }
}


#[derive(Debug, Clone)]
pub struct Redis {
    _inner: Arc<Mutex<HashMap<String, Value>>>
}

impl Redis {
    pub fn new() -> Self {
        Self {
            _inner: Arc::new(Mutex::new(HashMap::default()))
        }
    }
    pub fn snapshot(&self) -> HashMap<String, Value> {
        self._inner.lock().unwrap().clone()
    }

}

#[derive(Debug, Error, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum RedisError {
    #[error("Failed to lock internal mutex")]
    FailedToLockMutex
}


impl StateMachine for Redis {
    type Request = Command;
    type Response = RedisResponse<Value>;
    type Err = RedisError;

    fn _apply(&self, entry: Self::Request) -> Result<Self::Response, Self::Err> {
        match entry {
            Command::GET(cmd) => {
                if let Ok(guard) = self._inner.lock() {
                    match guard.get(&cmd.key) {
                        Some(value) => return Ok(RedisResponse::Success(value.clone())),
                        None => return Ok(RedisResponse::Success(Value::Null))
                    }
                }
                Err(RedisError::FailedToLockMutex)
            },
            Command::PUT(cmd) => {
                if let Ok(mut guard) = self._inner.lock() {
                    match guard.insert(cmd.key, Value::from(cmd.value)) {
                        Some(previous_value) => return Ok(RedisResponse::Success(previous_value)),
                        None => {
                            return Ok(RedisResponse::Success(Value::from("OK")))
                        }
                    }
                }
                Err(RedisError::FailedToLockMutex)
            },
            Command::DELETE(cmd) => {
                if let Ok(mut guard) = self._inner.lock() {
                    match guard.remove(&cmd.key) {
                        Some(previous_value) => return Ok(RedisResponse::Success(previous_value)),
                        None => {
                            return Ok(RedisResponse::Success(Value::from(0)))
                        }
                    }
                }
                Err(RedisError::FailedToLockMutex)
            }
        }
    }
}


#[cfg(test)]
pub mod tests {
    use std::error::Error;

    use super::*;
    use crate::*;
    use hashbrown::HashMap;


    type TestResult<T = ()> = core::result::Result<T, Box<dyn Error>>;

    #[test]
    fn serialize_commands() -> TestResult {
        let cmd = Command::GET(Get { key: "42".into() });
        let serialized = serde_json::to_string(&cmd)?;
        assert_eq!(serialized, r#"{"kind":"GET","key":"42"}"#);
        
        let cmd = Command::PUT(Put { key: "42".into(), value: "doggo".into() });
        let serialized = serde_json::to_string(&cmd)?;
        assert_eq!(serialized, r#"{"kind":"PUT","key":"42","value":"doggo"}"#);

        let cmd = Command::DELETE(Delete { key: "42".into() });
        let serialized = serde_json::to_string(&cmd)?;
        assert_eq!(serialized, r#"{"kind":"DELETE","key":"42"}"#);
        Ok(())
    }

    #[test]
    fn response_as_bytes() -> TestResult {
        let redis = Redis::new();
        let response = redis._apply(Command::GET(Get { key: "x".into() }) )?;
        let bytes: Bytes = response.into();
        assert_eq!(bytes.to_vec(), vec![123, 34, 83, 117, 99, 99, 101, 115, 115, 34, 58, 110, 117, 108, 108, 125]);
        Ok(())
    }

    #[test]
    fn from_str() -> TestResult {
        let cmds = vec![
            "GET x",
            "PUT x 2",
            "DELETE x"
        ];
        let expected_cmds = vec![
            Command::GET(Get {key: "x".to_owned() }),
            Command::PUT(Put {key: "x".to_owned(), value: Value::from(2) }),
            Command::DELETE(Delete {key: "x".to_owned() }),
        ];
        
        for idx in 0..cmds.len() {
            assert_eq!(expected_cmds[idx], Command::from_str(cmds[idx]).unwrap());
        }

        assert!(Command::from_str("GET ").is_err());
        assert!(Command::from_str("PUT ").is_err());
        assert!(Command::from_str("PUT x").is_err());
        assert!(Command::from_str("DELETE").is_err());
        assert!(Command::from_str("BLAH").is_err());
        assert!(Command::from_str("").is_err());

        Ok(())
    }

    #[test]
    fn to_str() -> TestResult {
        let expected_cmds = vec![
            "GET x",
            "PUT x 2",
            "DELETE x"
        ];
        let cmds = vec![
            Command::GET(Get {key: "x".to_owned() }),
            Command::PUT(Put {key: "x".to_owned(), value: Value::from(2) }),
            Command::DELETE(Delete {key: "x".to_owned() }),
        ];

        for idx in 0..cmds.len() {
            assert_eq!(expected_cmds[idx], cmds[idx].to_string());
        }
        Ok(())

    }

    #[test]
    fn apply_bytes() -> TestResult {
        let redis = Redis::new();
        let response = redis.apply(Bytes::from(Command::GET(Get { key: "x".into() })) )?;
        assert_eq!(response.to_vec(), Bytes::from(RedisResponse::Success(Value::Null)).to_vec());
        Ok(())
    }

    #[test]
    fn apply() -> TestResult {
        let redis = Redis::new();
        let response = redis._apply(Command::GET(Get { key: "x".into() }))?;
        assert_eq!(response, RedisResponse::Success(Value::Null));

        let response = redis._apply(Command::PUT(Put { key: "x".into(), value: Value::from("doggo")}))?;
        assert_eq!(response, RedisResponse::Success(Value::from("OK")));

        let response = redis._apply(Command::GET(Get { key: "x".into() }) )?;
        assert_eq!(response, RedisResponse::Success(Value::from("doggo")));

        let response = redis._apply(Command::DELETE(Delete { key: "x".into() }) )?;
        assert_eq!(response, RedisResponse::Success(Value::from("doggo")));

        let response = redis._apply(Command::DELETE(Delete { key: "x".into() }) )?;
        assert_eq!(response, RedisResponse::Success(Value::from(0)));
        
        let response = redis._apply(Command::PUT(Put { key: "x".into(), value: Value::from("doggo")}))?;
        assert_eq!(response, RedisResponse::Success(Value::from("OK")));

        let response = redis._apply(Command::PUT(Put { key: "x".into(), value: Value::from("doggo2")}))?;
        assert_eq!(response, RedisResponse::Success(Value::from("doggo")));

        assert_eq!(redis.snapshot(), HashMap::from_iter(vec![("x".into(), Value::from("doggo2"))]));

        let response = redis._apply(Command::PUT(Put { key: "x".into(), value: Value::from(r#""{"animal": "dog"}""#)}))?;
        assert_eq!(response, RedisResponse::Success(Value::from("doggo2")));

        assert_eq!(redis.snapshot(), HashMap::from_iter(vec![("x".into(), Value::from(r#""{"animal": "dog"}""#))]));

        Ok(())
    }
}