use maelstrom::protocol::{Message, MessageBody};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

pub enum Request {
    Transact { txn: Vec<Operation> },
    Init,
}

impl Request {
    pub fn new(req: &Message) -> Request {
        match req.get_type() {
            "init" => Request::Init,
            "txn" => {
                let ops = req
                    .body
                    .extra
                    .get("txn")
                    .expect("missing 'txn' params")
                    .as_array()
                    .expect("'txn' is not an array")
                    .iter()
                    .map(|it| {
                        let op = it.as_array().expect("'txn' operation is not an array");
                        let op_type = op
                            .get(0)
                            .unwrap()
                            .as_str()
                            .expect("'txn' first argument not i64");
                        match op_type {
                            "r" => {
                                let key = op
                                    .get(1)
                                    .unwrap()
                                    .as_i64()
                                    .expect("'txn' read provided no key");
                                Operation::Read {
                                    key: key,
                                    result: None,
                                }
                            }
                            "w" => {
                                let key = op
                                    .get(1)
                                    .unwrap()
                                    .as_i64()
                                    .expect("'txn' write provided no key");
                                let val = op
                                    .get(2)
                                    .unwrap()
                                    .as_i64()
                                    .expect("'txn' write provided no value");
                                Operation::Write {
                                    key: key,
                                    value: val,
                                }
                            }
                            _ => {
                                panic!("invalid operation: {}", op_type)
                            }
                        }
                    })
                    .collect();
                Request::Transact { txn: ops }
            }
            _ => panic!("invalid request: {}", req.get_type()),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Operation {
    Read { key: i64, result: Option<i64> },
    Write { key: i64, value: i64 },
}

impl Operation {
    pub fn serialize(&self) -> Vec<u8> {
        bincode::serialize(self).unwrap_or_else(|_| Vec::new())
    }

    pub fn deserialize(bytes: &[u8]) -> Result<Operation, String> {
        bincode::deserialize(bytes).map_err(|e| format!("Deserialization error: {}", e))
    }
}

impl Operation {}

impl Operation {
    pub fn as_arr(&self) -> Vec<Value> {
        let mut args = Vec::new();
        args.push(Value::String(self.get_type().to_string()));
        match self {
            Operation::Read { key, result } => {
                args.push(json!(key));
                if result.is_some() {
                    args.push(json!(result.unwrap()));
                }
            }
            Operation::Write { key, value } => {
                args.push(json!(key));
                args.push(json!(value));
            }
        }
        args
    }

    pub fn get_type(&self) -> &str {
        match self {
            Operation::Read { key: _, result: _ } => "r",
            Operation::Write { key: _, value: _ } => "w",
        }
    }
}

pub enum Response {
    TransactOk { txn: Vec<Operation> },
}

impl Response {
    pub fn as_body(&self) -> MessageBody {
        let mut extra = Map::new();
        match self {
            Response::TransactOk { txn } => {
                let args: Vec<Vec<Value>> = txn.iter().map(|op| op.as_arr()).collect();
                extra.insert("txn_".to_string(), json!(args));

                MessageBody::from_extra(extra).with_type("txn_ok")
            }
        }
    }
}
