use std::{
    collections::HashMap,
    hash::{DefaultHasher, Hash, Hasher},
    sync::Arc,
};

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
        let mut node_ids = runtime.nodes().to_vec();
        node_ids.sort();

        let req_type = RequestType::from_msg(&req);
        if req_type.is_some() {
            let resp = match req_type.unwrap() {
                RequestType::Send { key, value } => {
                    self.kafka
                        .send(
                            &runtime,
                            node_ids,
                            runtime.node_id().to_string(),
                            key,
                            value,
                        )
                        .await
                }
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

    async fn send(
        &self,
        runtime: &Runtime,
        node_ids: Vec<String>,
        cur_node_id: String,
        key: String,
        value: i64,
    ) -> ResponseType {
        let target_node = self.route_msg(node_ids, key.clone());

        if target_node == cur_node_id {
            self.send_myself(key, value).await
        } else {
            self.forward_send(runtime, target_node, key, value).await
        }
    }

    async fn forward_send(
        &self,
        runtime: &Runtime,
        target_node: String,
        key: String,
        value: i64,
    ) -> ResponseType {
        info!(
            "Forwarding send to node {} for key={} value={}",
            target_node, key, value
        );

        let req = RequestType::Send {
            key: key,
            value: value,
        };
        let (ctx, ctx_hdl) = Context::new();
        let call_res = runtime
            .call(ctx, target_node.clone(), req.to_body())
            .await
            .expect(format!("Failed to forward msg to dest nod={}", target_node.clone()).as_str());
        ctx_hdl.cancel();

        ResponseType::from_body(call_res.body).expect("Response body not type send_ok")
    }

    async fn send_myself(&self, key: String, value: i64) -> ResponseType {
        let store = self.store.write().await;
        let (ctx, ctx_hdl) = Context::new();

        /* Get latest offset of key */
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

        /* Save latest offset to database */
        let (ctx, ctx_hdl) = Context::new();
        let _ = store.put(ctx, offset_key.to_str(), offset)
        .await
        .expect("failed to update offset");
        ctx_hdl.cancel();

        info!("Put log key={} value={} offset={}", key, value, offset);

        let log_key = StorageKey::Entry { key: key.clone() };

        /* Get current entries from database */
        let (ctx, ctx_hdl) = Context::new();
        let entries_res = store.get(ctx, log_key.to_str()).await;
        ctx_hdl.cancel();

        let mut entries;
        if let Err(_) = entries_res {
            entries = Vec::new();
        } else {
            entries = Entry::parse_str(entries_res.unwrap());
        }

        entries.push(Entry::key_value(offset as usize, value));

        /* Save back to entries to database */
        let (ctx, ctx_hdl) = Context::new();
        let _ = store
            .put(ctx, log_key.to_str(), Entry::list_to_str(entries))
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
            let mut key_msgs = Vec::new();
            let (ctx, ctx_hdl) = Context::new();
            let entry_key = StorageKey::Entry { key: key.clone() };
            let value_res = store.get(ctx, entry_key.to_str()).await;
            if let Err(_) = value_res {
                ctx_hdl.cancel();
                msgs.insert(key.clone(), key_msgs);
                continue;
            }
            ctx_hdl.cancel();

            let entries = Entry::parse_str(value_res.unwrap());

            for entry in entries {
                if entry.offset >= offset {
                    key_msgs.push(entry.as_arr());
                }
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

    fn route_msg(&self, node_ids: Vec<String>, key: String) -> String {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let node_id = hasher.finish() as usize % node_ids.len();
        node_ids
            .get(node_id)
            .expect(format!("Hash out of bound node_id={} key={}", node_id, key).as_str())
            .clone()
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

    fn to_body(&self) -> MessageBody {
        let mut extra = Map::new();
        let msg_type;
        match self {
            RequestType::Send { key, value } => {
                extra.insert("msg".to_string(), json!(value));
                extra.insert("key".to_string(), json!(key));
                msg_type = "send";
            }
            RequestType::Poll { offsets } => {
                extra.insert("offsets".to_string(), json!(offsets));
                msg_type = "poll";
            }
            RequestType::Commit { offsets } => {
                extra.insert("offsets".to_string(), json!(offsets));
                msg_type = "commit_offsets";
            }
            RequestType::ListOffsets { keys } => {
                extra.insert("keys".to_string(), json!(keys));
                msg_type = "list_committed_offsets";
            }
        }
        MessageBody::from_extra(extra).with_type(msg_type)
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

    fn from_body(msg_body: MessageBody) -> Option<ResponseType> {
        let body = msg_body.extra;
        match msg_body.typ.as_str() {
            "send_ok" => {
                let offset = body
                    .get("offset")
                    .expect("'offset' not found in 'send_ok'")
                    .as_u64()
                    .expect("'offset' is not u64") as usize;
                Some(ResponseType::SendOk { offset })
            }
            "list_committed_offsets_ok" => {
                let offsets = body
                    .get("offsets")
                    .expect("'offsets' not found in 'list_committed_offsets_ok'")
                    .as_object()
                    .expect("'offsets' is not a map")
                    .iter()
                    .map(|e| {
                        (
                            e.0.as_str().to_string(),
                            e.1.as_u64().expect("'offset value is not u64") as usize,
                        )
                    })
                    .collect();
                Some(ResponseType::ListOffsetsOk { offsets })
            }
            "commit_offsets_ok" => Some(ResponseType::CommitOk),
            "poll_ok" => {
                let msgs = body
                    .get("msgs")
                    .expect("'msgs' not found in 'poll_ok'")
                    .as_object()
                    .expect("'msgs' is not a map")
                    .iter()
                    .map(|e| {
                        let key = e.0.as_str().to_string();
                        let msg_arrays =
                            e.1.as_array()
                                .expect("'msg value is not array")
                                .iter()
                                .map(|arr| {
                                    let pair = arr.as_array().expect("'msg entry is not array");
                                    [
                                        pair[0].as_i64().expect("'offset is not i64"),
                                        pair[1].as_i64().expect("'value is not i64"),
                                    ]
                                })
                                .collect();
                        (key, msg_arrays)
                    })
                    .collect();
                Some(ResponseType::PollOk { msgs })
            }
            _ => None,
        }
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

    fn to_string(&self) -> String {
        format!("o={}_v={}", self.offset, self.value)
    }

    fn from_str(str: String) -> Entry {
        let parts: Vec<&str> = str.split("_").collect();
        let offset_part = parts[0].replace("o=", "");
        let offset_val = offset_part.parse::<usize>().expect("Invalid offset");

        let value_part = parts[1].replace("v=", "");
        let value_val = value_part.parse::<i64>().expect("Invalid value");

        Entry {
            offset: offset_val,
            value: value_val,
        }
    }

    fn parse_str(str: String) -> Vec<Entry> {
        if str.is_empty() {
            return Vec::new();
        }
        str.split(",")
            .map(|s| Entry::from_str(s.to_string()))
            .collect()
    }

    fn list_to_str(entries: Vec<Entry>) -> String {
        entries
            .into_iter()
            .map(|entry| entry.to_string())
            .collect::<Vec<String>>()
            .join(",")
    }
}

enum StorageKey {
    Entry { key: String },
    Offset { key: String },
    Commit { key: String },
}

impl StorageKey {
    fn to_str(&self) -> String {
        match self {
            StorageKey::Entry { key } => format!("entry_{}", key),
            StorageKey::Offset { key } => format!("latestoffset_{}", key),
            StorageKey::Commit { key } => format!("committed_{}", key),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_entry_to_string() {
        let entry = Entry::key_value(0, 1);
        assert_eq!(entry.to_string(), "o=0_v=1");

        let entry2 = Entry::key_value(1, 2);
        assert_eq!(entry2.to_string(), "o=1_v=2");
    }

    #[test]
    fn test_entry_from_str() {
        let entry = Entry::from_str("o=0_v=1".to_string());
        assert_eq!(entry.offset, 0);
        assert_eq!(entry.value, 1);

        let entry2 = Entry::from_str("o=1_v=2".to_string());
        assert_eq!(entry2.offset, 1);
        assert_eq!(entry2.value, 2);
    }

    #[test]
    fn test_entry_list_to_str() {
        let entries = vec![
            Entry::key_value(0, 1),
            Entry::key_value(1, 2),
        ];
        let result = Entry::list_to_str(entries);
        assert_eq!(result, "o=0_v=1,o=1_v=2");
    }

    #[test]
    fn test_entry_parse_str() {
        let input = "o=0_v=1,o=1_v=2".to_string();
        let entries = Entry::parse_str(input);
        
        assert_eq!(entries.len(), 2);
        
        assert_eq!(entries[0].offset, 0);
        assert_eq!(entries[0].value, 1);
        
        assert_eq!(entries[1].offset, 1);
        assert_eq!(entries[1].value, 2);
    }

    #[test]
    fn test_entry_parse_str_empty() {
        let input = "".to_string();
        let entries = Entry::parse_str(input);
        assert_eq!(entries.len(), 0);
    }

    #[test]
    fn test_entry_roundtrip() {
        let original_entries = vec![
            Entry::key_value(0, 1),
            Entry::key_value(1, 2),
            Entry::key_value(5, 42),
        ];
        
        // Serialize to string
        let serialized = Entry::list_to_str(original_entries.clone());
        
        // Deserialize back
        let deserialized = Entry::parse_str(serialized);
        
        // Verify they match
        assert_eq!(original_entries.len(), deserialized.len());
        for (orig, deser) in original_entries.iter().zip(deserialized.iter()) {
            assert_eq!(orig.offset, deser.offset);
            assert_eq!(orig.value, deser.value);
        }
    }

    #[test]
    fn test_entry_as_arr() {
        let entry = Entry::key_value(5, 42);
        let arr = entry.as_arr();
        assert_eq!(arr, [5, 42]);
    }
}
