use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use log::info;
use maelstrom::{
    Node, Result, Runtime, done,
    protocol::{Message, MessageBody},
};
use serde_json::{Map, json};
use tokio::sync::{Mutex, RwLock};

fn main() -> Result<()> {
    Runtime::init(try_main())
}

async fn try_main() -> Result<()> {
    let runtime = Runtime::new();

    let handler = Arc::new(Handler::new());

    runtime.with_handler(handler).run().await
}

#[derive(Clone)]
struct Handler {
    kafka: Arc<Kafka>,
}

#[async_trait]
impl Node for Handler {
    async fn process(&self, runtime: Runtime, req: Message) -> Result<()> {
        let req_type = RequestType::from_msg(&req);
        if req_type.is_some() {
            let resp = match req_type.unwrap() {
                RequestType::Send { key, value } => self.kafka.send(key, value).await,
                RequestType::Poll { offsets } => self.kafka.poll(offsets).await,
                RequestType::Commit { offsets } => self.kafka.commit(offsets).await,
                RequestType::ListOffsets { keys } => self.kafka.list_offsets(keys).await,
            };
            return runtime.reply(req, resp.into_body()).await;
        }
        done(runtime, req)
    }
}

impl Handler {
    fn new() -> Handler {
        Handler {
            kafka: Arc::new(Kafka::new()),
        }
    }
}

struct Kafka {
    log: Arc<Mutex<Log>>,
    offsets: Arc<RwLock<HashMap<String, usize>>>,

}

impl Kafka {
    fn new() -> Kafka {
        Kafka {
            log: Arc::new(Mutex::new(Log::new())),
            offsets: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    async fn send(&self, key: String, value: i64) -> ResponseType {
        let mut log = self.log.lock().await;
        let entry = log.append(&key, value);
        info!(
            "Sent Kafka key={} value={} offset={}",
            key, value, entry.offset
        );
        ResponseType::SendOk {
            offset: entry.offset,
        }
    }

    async fn poll(&self, offsets: HashMap<String, usize>) -> ResponseType {
        let log = self.log.lock().await;
        info!("Polling Kafka offsets={:?}", offsets);

        let mut msgs = HashMap::new();
        for (key, offset) in offsets {
            let entries = log.poll(key.as_str(), offset);
            msgs.insert(key, entries.iter().map(|e| e.as_arr()).collect());
        }

        ResponseType::PollOk { msgs: msgs }
    }

    async fn commit(&self, offsets: HashMap<String, usize>) -> ResponseType {
        let mut map = self.offsets.write().await;
        info!("Commit Kafka offsets={:?}", offsets);

        for (key, offset) in offsets {
            map.insert(key, offset);
        }
        ResponseType::CommitOk
    }

    async fn list_offsets(&self, keys: Vec<String>) -> ResponseType {
        let map = self.offsets.read().await;

        let mut result = HashMap::new();
        for key in keys.clone() {
            let cl_key = key.clone();
            result.insert(key, map.get(cl_key.as_str()).cloned().unwrap_or(0));
        }

        info!("List Kafka offsets keys={:?} offsets={:?}", keys, result);
        ResponseType::ListOffsetsOk { offsets: result }
    }
}

enum RequestType {
    Send { key: String, value: i64 },
    Poll { offsets: HashMap<String, usize> },
    Commit { offsets: HashMap<String, usize> },
    ListOffsets { keys: Vec<String> },
}

impl RequestType {
    fn from_msg(msg: &Message) -> Option<RequestType> {
        let extra = &msg.body.extra;

        match msg.get_type() {
            "send" => {
                let msg = extra
                    .get("msg")
                    .expect("'msg' not included in 'send' msg")
                    .as_i64()
                    .expect("'msg' is not i64");
                let key = extra
                    .get("key")
                    .expect("'key' not included in 'send' msg")
                    .as_str()
                    .expect("'key' is not a str");
                Some(RequestType::Send {
                    key: key.to_string(),
                    value: msg,
                })
            }
            "poll" => {
                let offsets = extra
                    .get("offsets")
                    .expect("'offsets' not found in 'poll'")
                    .as_object()
                    .expect("'offsets' is not a map")
                    .iter()
                    .map(|e| {
                        (
                            e.0.as_str().to_string(),
                            e.1.as_i64().expect("'offset value is not i64") as usize,
                        )
                    })
                    .collect();

                Some(RequestType::Poll { offsets: offsets })
            }
            "commit_offsets" => {
                let offsets = extra
                    .get("offsets")
                    .expect("'offsets' not found in 'poll'")
                    .as_object()
                    .expect("'offsets' is not a map")
                    .iter()
                    .map(|e| {
                        (
                            e.0.as_str().to_string(),
                            e.1.as_i64().expect("'offset value is not i64") as usize,
                        )
                    })
                    .collect();

                Some(RequestType::Commit { offsets: offsets })
            }
            "list_committed_offsets" => {
                let keys = extra
                    .get("keys")
                    .expect("'keys' not included in 'list_committed_offsets'")
                    .as_array()
                    .expect("'keys' is not arr")
                    .iter()
                    .map(|v| v.as_str().expect("'keys' element is not str").to_string())
                    .collect();

                Some(RequestType::ListOffsets { keys: keys })
            }
            _ => None,
        }
    }
}

enum ResponseType {
    SendOk {
        offset: usize,
    },
    ListOffsetsOk {
        offsets: HashMap<String, usize>,
    },
    CommitOk,
    PollOk {
        msgs: HashMap<String, Vec<[i64; 2]>>,
    },
}

impl ResponseType {
    fn into_body(&self) -> MessageBody {
        let mut extra = Map::new();
        let msg_type = match self {
            ResponseType::SendOk { offset } => {
                extra.insert("offset".to_string(), json!(offset));
                "send_ok"
            }
            ResponseType::ListOffsetsOk { offsets } => {
                extra.insert("offsets".to_string(), json!(offsets));
                "list_committed_offsets_ok"
            }
            ResponseType::CommitOk => "commit_offsets_ok",
            ResponseType::PollOk { msgs } => {
                extra.insert("msgs".to_string(), json!(msgs));
                "poll_ok"
            }
        };
        MessageBody::from_extra(extra).with_type(msg_type)
    }
}

#[derive(Default, Debug, Clone)]
struct Log {
    queue: HashMap<String, Vec<Entry>>,
}

impl Log {
    fn new() -> Log {
        Log {
            queue: HashMap::new(),
        }
    }

    fn append(&mut self, key: &str, value: i64) -> Entry {
        if !self.queue.contains_key(key) {
            let mut start = Vec::new();
            let entry = Entry::key_value(0, value);
            start.push(entry.clone());
            self.queue.insert(key.to_string(), start);
            return entry;
        }

        let curr = self.queue.get_mut(key).unwrap();
        let entry = Entry::key_value(curr.len(), value);
        curr.push(entry.clone());
        return entry;
    }

    fn poll(&self, key: &str, offset: usize) -> Vec<Entry> {
        if self.queue.contains_key(key) {
            let part = self.queue.get(key);
            if part.is_none() {
                return Vec::new();
            }
            let part_w = part.unwrap();
            if offset >= part_w.len() {
                return Vec::new();
            }

            return part_w[offset..].to_vec();
        }

        Vec::new()
    }
}

#[derive(Clone, Debug)]
struct Entry {
    offset: usize,
    value: i64,
}

impl Entry {
    fn key_value(offset: usize, value: i64) -> Entry {
        Entry {
            offset: offset,
            value: value,
        }
    }

    fn as_arr(&self) -> [i64; 2] {
        [self.offset as i64, self.value]
    }
}
