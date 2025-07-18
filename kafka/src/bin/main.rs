use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use log::info;
use maelstrom::{
    Node, Result, Runtime, done,
    kv::{KV, Storage, lin_kv},
    protocol::{Message, MessageBody},
};
use serde_json::{Map, json};
use tokio::sync::RwLock;
use tokio_context::context::Context;

fn main() -> Result<()> {
    Runtime::init(try_main())
}

async fn try_main() -> Result<()> {
    let runtime = Runtime::new();

    let handler = Arc::new(Handler::new(runtime.clone()));

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
    fn new(runtime: Runtime) -> Handler {
        Handler {
            kafka: Arc::new(Kafka::new(runtime)),
        }
    }
}

struct Kafka {
    store: Arc<RwLock<Storage>>,
}

impl Kafka {
    fn new(runtime: Runtime) -> Kafka {
        Kafka {
            store: Arc::new(RwLock::new(lin_kv(runtime))),
        }
    }

    async fn send(&self, key: String, value: i64) -> ResponseType {
        let store = self.store.write().await;
        let (ctx, ctx_hdl) = Context::new();
        let offset_key = StorageKey::Offset { key: key.clone() };
        let latest_offset = store.get(ctx, offset_key.to_str()).await;

        let mut offset: i32;
        if let Err(_) = latest_offset {
            offset = 0;
        } else {
            offset = latest_offset.unwrap();
            offset += 1;
        }
        ctx_hdl.cancel();

        loop {
            let (ctx, ctx_hdl) = Context::new();
            let cas_res = store
                .cas(ctx, offset_key.to_str(), offset - 1, offset, true)
                .await;
            if let Err(_) = cas_res {
                println!("Outdated offset local_cur={}", offset - 1);
                offset += 1;
                ctx_hdl.cancel();
                continue;
            }
            ctx_hdl.cancel();
            break;
        }

        info!("Put log key={} value={} offset={}", key, value, offset);

        let log_key = StorageKey::Entry {
            key: key.clone(),
            offset: offset as usize,
        };

        let (ctx, ctx_hdl) = Context::new();
        let _ = store
            .put(ctx, log_key.to_str(), value)
            .await
            .expect("Failed to put key into lin-kv");

        ctx_hdl.cancel();

        ResponseType::SendOk {
            offset: offset as usize,
        }
    }

    async fn poll(&self, offsets: HashMap<String, usize>) -> ResponseType {
        let store = self.store.read().await;
        info!("Polling Kafka offsets={:?}", offsets);

        let mut msgs = HashMap::new();
        for (key, offset) in offsets {
            let (ctx, ctx_hdl) = Context::new();
            let latest_key = StorageKey::Offset { key: key.clone() };
            let latest = store.get(ctx, latest_key.to_str()).await;
            if let Err(_) = latest {
                msgs.insert(key.clone(), Vec::new());
                ctx_hdl.cancel();
                continue;
            }
            let latest_offset: usize = latest.unwrap();
            ctx_hdl.cancel();

            let mut key_msgs = Vec::new();
            for offset in offset..(latest_offset + 1) {
                let (ctx, ctx_hdl) = Context::new();
                let entry_key = StorageKey::Entry {
                    key: key.clone(),
                    offset: offset,
                };
                let value_res = store.get(ctx, entry_key.to_str()).await;
                if let Err(_) = value_res {
                    ctx_hdl.cancel();
                    break;
                }
                let entry = Entry::key_value(offset, value_res.unwrap());
                key_msgs.push(entry.as_arr());
                ctx_hdl.cancel();
            }

            msgs.insert(key.clone(), key_msgs);
        }

        ResponseType::PollOk { msgs: msgs }
    }

    async fn commit(&self, offsets: HashMap<String, usize>) -> ResponseType {
        let store = self.store.write().await;
        info!("Commit Kafka offsets={:?}", offsets);

        for (key, offset) in offsets {
            let (ctx, ctx_hdl) = Context::new();
            let commit_key = StorageKey::Commit { key: key.clone() };

            let committed;
            let curr_committed = store.get(ctx, commit_key.to_str()).await;
            if let Err(_) = curr_committed {
                committed = offset;
            } else {
                committed = curr_committed.unwrap();
            }

            if committed > offset {
                info!(
                    "Provided key={} offset={} outdated curr={}, skipping",
                    key.clone(),
                    offset,
                    committed
                );
            } else {
                let (ctx, ctx_hdl) = Context::new();
                let commit_key = StorageKey::Commit { key: key.clone() };
                let _ = store
                    .put(ctx, commit_key.to_str(), offset)
                    .await
                    .expect(format!("Failed to put latest commit for key '{}'", key).as_str());
                ctx_hdl.cancel();
            }
            ctx_hdl.cancel();
        }
        ResponseType::CommitOk
    }

    async fn list_offsets(&self, keys: Vec<String>) -> ResponseType {
        let store = self.store.read().await;

        let mut result = HashMap::new();
        for key in keys.clone() {
            let (ctx, ctx_hdl) = Context::new();
            let commit_key = StorageKey::Commit { key: key.clone() };
            let commit_res = store.get(ctx, commit_key.to_str()).await;
            ctx_hdl.cancel();
            if let Err(_) = commit_res {
                result.insert(key.clone(), 0);
            } else {
                result.insert(key.clone(), commit_res.unwrap());
            }
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

enum StorageKey {
    Entry { key: String, offset: usize },
    Offset { key: String },
    Commit { key: String },
}

impl StorageKey {
    fn to_str(&self) -> String {
        match self {
            StorageKey::Entry { key, offset } => format!("entry_{}_{}", key, offset),
            StorageKey::Offset { key } => format!("latestoffset_{}", key),
            StorageKey::Commit { key } => format!("committed_{}", key),
        }
    }
}
