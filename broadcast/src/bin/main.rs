use async_trait::async_trait;
use futures_util::lock::Mutex;
use log::info;
use maelstrom::protocol::MessageBody;
use maelstrom::{Node, Result, Runtime, done, protocol::Message};
use rand::random;
use serde_json::{Map, json};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::time;

fn main() -> Result<()> {
    Runtime::init(try_main())
}

async fn try_main() -> Result<()> {
    let runtime = Runtime::new();
    let bc_handler = BroadcastHandler::new();
    let handler = Arc::new(Handler::new(bc_handler));
    runtime.with_handler(handler).run().await
}

#[derive(Clone)]
struct Handler {
    bc_handler: Arc<Mutex<BroadcastHandler>>,
}

#[async_trait]
impl Node for Handler {
    async fn process(&self, runtime: Runtime, req: Message) -> Result<()> {
        let req_clone = req.clone();
        let mut bc_handler = self.bc_handler.lock().await;

        if let Ok(_) = bc_handler.handle(&runtime, req).await {
            return Ok(());
        }
        done(runtime, req_clone)
    }
}

impl Handler {
    fn new(bc_handler: BroadcastHandler) -> Handler {
        return Handler {
            bc_handler: Arc::new(Mutex::new(bc_handler)),
        };
    }
}

struct BroadcastHandler {
    topo: HashMap<String, Vec<String>>,
    values: Arc<RwLock<HashSet<u64>>>,
}

impl BroadcastHandler {
    pub fn new() -> BroadcastHandler {
        let handler = BroadcastHandler {
            topo: HashMap::new(),
            values: Arc::new(RwLock::new(HashSet::new())),
        };
        handler
    }

    pub async fn handle(&mut self, runtime: &Runtime, req: Message) -> Result<()> {
        match req.get_type() {
            "broadcast" => self.hdl_broadcast(runtime, req).await,
            "read" => self.hdl_read(runtime, req).await,
            "topology" => self.hdl_topology(runtime, req).await,
            "gossip" => self.hdl_gossip(runtime, req).await,
            "init" => {
                Gossip::start(runtime.clone(), self.values.clone());
                Ok(())
            }
            _ => Err("unknown request type".into()),
        }
    }

    async fn hdl_broadcast(&mut self, runtime: &Runtime, req: Message) -> Result<()> {
        let mut values = self.values.write().await;

        let dests = self.topo.get(runtime.node_id());

        let msg_body = &req.body;
        let msg_num = msg_body
            .extra
            .get("message")
            .expect("Message was not sent")
            .as_u64()
            .expect("Message is not i64");

        if values.insert(msg_num) {
            if dests.is_some() {
                let dest_nodes = dests.unwrap().to_vec();
                for dest in dest_nodes.iter() {
                    let mut extra = Map::new();
                    extra.insert(String::from("message"), json!(msg_num));

                    let msg_body = MessageBody::from_extra(extra)
                        .and_msg_id(req.body.msg_id)
                        .with_type("broadcast");

                    let _ = runtime.send_async(dest, msg_body);

                    info!(
                        "Sent msg_id req={} dest={:?} message={}",
                        req.body.msg_id, dest_nodes, msg_num,
                    );
                }
            }
        }
        return runtime.reply_ok(req).await;
    }

    async fn hdl_read(&self, runtime: &Runtime, req: Message) -> Result<()> {
        let values = self.values.read().await;

        let mut extra = Map::new();
        extra.insert(String::from("messages"), json!(values.clone()));

        let msg_body = MessageBody::from_extra(extra).with_type("read_ok");

        return runtime.reply(req, msg_body).await;
    }

    async fn hdl_topology(&mut self, runtime: &Runtime, req: Message) -> Result<()> {
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
                        self.topo.insert(node_id.clone(), neighbor_strings);
                    }
                }
            }
        }
        info!("Broadcast topology: {:?}", self.topo);

        return runtime.reply_ok(req).await;
    }

    async fn hdl_gossip(&mut self, runtime: &Runtime, req: Message) -> Result<()> {
        let mut values = self.values.write().await;
        req.body
            .extra
            .get("state")
            .expect("Gossip must include state")
            .as_array()
            .inspect(|s| info!("Received state: {:?}", s))
            .expect("State must be an array")
            .iter()
            .for_each(|v| {
                let msg = v.as_u64();
                values.insert(msg.expect("State item is not u64"));
            });
        runtime.reply_ok(req).await
    }
}

#[derive(Debug, Default)]
struct Gossip {}

impl Gossip {
    fn start(runtime: Runtime, values_ref: Arc<RwLock<HashSet<u64>>>) {
        let neighbors = Vec::from_iter(runtime.neighbours().cloned());

        if neighbors.is_empty() {
            info!("Neighbor is empty");
            return;
        }

        info!("Neighbors: {:?}", neighbors);

        tokio::spawn(async move {
            info!("Gossip starts");

            let mut msg_id = 0;
            loop {
                let rnd = random::<usize>() % neighbors.len();
                let dest_id = neighbors.get(rnd).unwrap();

                let curr_values = values_ref.read().await;
                let mut extra = Map::new();
                let nodes = curr_values.iter().cloned().collect::<Vec<_>>();

                extra.insert(String::from("state"), json!(nodes));
                let msg = MessageBody::from_extra(extra)
                    .with_type("gossip")
                    .and_msg_id(msg_id);

                info!("Gossip to dest_id={} msg_id={}", dest_id, msg_id);

                let _ = runtime.send_async(dest_id, msg);
                msg_id += 1;

                time::sleep(Duration::from_millis(100)).await;
            }
        });
    }
}
