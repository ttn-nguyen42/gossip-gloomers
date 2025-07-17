use async_trait::async_trait;
use futures_util::future::join_all;
use maelstrom::protocol::MessageBody;
use maelstrom::{Node, Result, Runtime, done, protocol::Message};
use serde_json::{Map, json};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

fn main() -> Result<()> {
    Runtime::init(try_main())
}

async fn try_main() -> Result<()> {
    let runtime = Runtime::new();
    let bc_handler = BroadcastHandler::new(runtime.node_id());
    let handler = Arc::new(Handler::new(bc_handler));
    runtime.with_handler(handler).run().await
}

#[derive(Clone)]
struct Handler {
    bc_handler: BroadcastHandler,
}

#[async_trait]
impl Node for Handler {
    async fn process(&self, runtime: Runtime, req: Message) -> Result<()> {
        let req_clone = req.clone();
        if let Ok(_) = self.bc_handler.handle(&runtime, req).await {
            return Ok(());
        }
        done(runtime, req_clone)
    }
}

impl Handler {
    fn new(bc_handler: BroadcastHandler) -> Handler {
        return Handler {
            bc_handler: bc_handler,
        };
    }
}

#[derive(Clone)]
struct BroadcastHandler {
    node_id: String,
    topo: Arc<RwLock<HashMap<String, Vec<String>>>>,
    values: Arc<RwLock<Vec<i64>>>,
}

impl BroadcastHandler {
    pub fn new(node_id: &str) -> BroadcastHandler {
        BroadcastHandler {
            node_id: String::from(node_id),
            topo: Arc::new(RwLock::new(HashMap::new())),
            values: Arc::new(RwLock::new(Vec::new())),
        }
    }

    pub async fn handle(&self, runtime: &Runtime, req: Message) -> Result<()> {
        match req.get_type() {
            "broadcast" => self.hdl_broadcast(runtime, req).await,
            "read" => self.hdl_read(runtime, req).await,
            "topology" => self.hdl_topology(runtime, req).await,
            _ => Err("unknown request type".into()),
        }
    }

    async fn hdl_broadcast(&self, runtime: &Runtime, req: Message) -> Result<()> {
        let topo_guard = self.topo.read().await;
        let dests = topo_guard.get(self.node_id.as_str());

        let msg_body = &req.body.extra;
        let value = msg_body
            .get("message")
            .expect("Message was not sent")
            .as_i64()
            .expect("Message is not i64");

        if dests.is_some() {
            let mut futs = Vec::new();
            for dest in dests.unwrap() {
                futs.push(runtime.send(dest, msg_body.clone()));
            }
            join_all(futs).await;
        }

        drop(topo_guard);

        self.values.write().await.push(value);

        return runtime.reply_ok(req).await;
    }

    async fn hdl_read(&self, runtime: &Runtime, req: Message) -> Result<()> {
        let values_guard = self.values.read().await;
        let mut json_values = Vec::new();
        values_guard
            .iter()
            .for_each(|it| json_values.push(json!(it)));

        let mut extra = Map::new();
        extra.insert(String::from("messages"), json!(json_values));
        let msg_body = MessageBody::from_extra(extra)
            .and_msg_id(req.body.msg_id)
            .with_reply_to(req.body.in_reply_to)
            .with_type("read_ok");

        return runtime.reply(req, msg_body).await;
    }

    async fn hdl_topology(&self, runtime: &Runtime, req: Message) -> Result<()> {
        let mut cur_topo = self.topo.write().await;
        let extra = &req.body.extra;
        if let Some(topo_map) = extra.get("topology") {
            if let Some(topo_obj) = topo_map.as_object() {
                for (node_id, neighbors) in topo_obj {
                    if let Some(neighbor_array) = neighbors.as_array() {
                        let neighbor_strings: Vec<String> = neighbor_array
                            .iter()
                            .filter_map(|v| v.as_str())
                            .map(|s| s.to_string())
                            .collect();
                        cur_topo.insert(node_id.clone(), neighbor_strings);
                    }
                }
            }
        }

        return runtime.reply_ok(req).await;
    }
}
