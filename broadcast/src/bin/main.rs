use async_trait::async_trait;
use futures_util::lock::Mutex;
use log::info;
use maelstrom::protocol::MessageBody;
use maelstrom::{Node, Result, Runtime, done, protocol::Message};
use serde_json::{Map, json};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::mpsc::{Receiver, Sender, channel};

fn main() -> Result<()> {
    Runtime::init(try_main())
}

async fn try_main() -> Result<()> {
    let runtime = Runtime::new();
    let bc_handler = BroadcastHandler::new(&runtime);
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
    values: HashSet<i64>,
    tx: Sender<BroadcastMessage>,
}

#[derive(Debug)]
struct BroadcastMessage {
    dests: Vec<String>,
    message: i64,
    msg_id: u64,
}

impl BroadcastMessage {
    pub fn into_body(&self) -> MessageBody {
        let mut extra = Map::new();
        extra.insert(String::from("message"), json!(self.message));

        MessageBody::from_extra(extra)
            .and_msg_id(self.msg_id)
            .with_type("broadcast")
    }
}

impl BroadcastHandler {
    pub fn new(runtime: &Runtime) -> BroadcastHandler {
        let (tx, rx) = channel(1024);

        let handler = BroadcastHandler {
            topo: HashMap::new(),
            values: HashSet::new(),
            tx: tx,
        };

        handler.consume_forward(rx, runtime.clone());
        handler
    }

    fn consume_forward(&self, mut rx: Receiver<BroadcastMessage>, runtime: Runtime) {
        tokio::spawn(async move {
            while let Some(qmsg) = rx.recv().await {
                let msg_body = qmsg.into_body();
                for dest in qmsg.dests.iter() {
                    let send_res = runtime.send_async(dest, &msg_body);
                    if let Err(err) = send_res {
                        info!("Failed to broadcast message: {}", err)
                    }
                    info!("Broadcast ok to dest={} message={}", dest, qmsg.message)
                }
            }
        });
    }

    pub async fn handle(&mut self, runtime: &Runtime, req: Message) -> Result<()> {
        match req.get_type() {
            "broadcast" => self.hdl_broadcast(runtime, req).await,
            "read" => self.hdl_read(runtime, req).await,
            "topology" => self.hdl_topology(runtime, req).await,
            _ => Err("unknown request type".into()),
        }
    }

    async fn hdl_broadcast(&mut self, runtime: &Runtime, req: Message) -> Result<()> {
        let dests = self.topo.get(runtime.node_id());

        let msg_body = &req.body;
        let msg_num = msg_body
            .extra
            .get("message")
            .expect("Message was not sent")
            .as_i64()
            .expect("Message is not i64");

        if self.values.insert(msg_num) {
            if dests.is_some() {
                let dest_nodes = dests.unwrap().to_vec();
                info!(
                    "Queuing msg_id req={} resp={} to dest={:?} message={}",
                    req.body.msg_id, msg_body.msg_id, dest_nodes, msg_num,
                );
                let qmsg = BroadcastMessage {
                    dests: dest_nodes,
                    message: msg_num,
                    msg_id: req.body.msg_id,
                };
                let _ = self.tx.send(qmsg).await;
            }
        }

        return runtime.reply_ok(req).await;
    }

    async fn hdl_read(&self, runtime: &Runtime, req: Message) -> Result<()> {
        let mut extra = Map::new();
        extra.insert(String::from("messages"), json!(self.values));

        let msg_body = MessageBody::from_extra(extra)
            .and_msg_id(req.body.msg_id)
            .with_reply_to(req.body.in_reply_to)
            .with_type("read_ok");

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
}
