use std::{cmp::max, collections::HashMap, ops::Add, sync::Arc, time::Duration};

use async_trait::async_trait;
use log::info;
use maelstrom::{
    Node, Result, Runtime, done,
    protocol::{Message, MessageBody},
};
use serde_json::{Map, Value, json};
use tokio::{sync::RwLock, time};

fn main() -> Result<()> {
    Runtime::init(try_main())
}

async fn try_main() -> Result<()> {
    let runtime = Runtime::new();
    let counter = Counter::new();
    let handler = Arc::new(Handler::new(Arc::new(counter)));
    runtime.with_handler(handler).run().await
}

#[derive(Clone)]
struct Handler {
    counter: Arc<Counter>,
}

#[async_trait]
impl Node for Handler {
    async fn process(&self, runtime: Runtime, req: Message) -> Result<()> {
        let req_type = RequestType::from_str(req.get_type(), &req.body.extra);
        if req_type.is_some() {
            let result = self
                .counter
                .handle(&runtime, req_type.unwrap(), req.clone())
                .await;
            if let Ok(_) = result {
                return Ok(());
            }
        }
        done(runtime, req)
    }
}

impl Handler {
    fn new(counter: Arc<Counter>) -> Self {
        Handler { counter: counter }
    }
}

#[derive(Clone, Default)]
struct Counter {
    counter: Arc<RwLock<HashMap<String, i64>>>,
}

impl Counter {
    fn new() -> Counter {
        Counter {
            counter: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    async fn handle(&self, runtime: &Runtime, req_type: RequestType, req: Message) -> Result<()> {
        let neighbors = runtime.neighbours().cloned().collect();
        match req_type {
            RequestType::Add { delta } => {
                info!("Add delta={}", delta);
                self.add(runtime.node_id(), delta).await;
                self.replicate(runtime, neighbors, delta).await;
                return runtime.reply_ok(req).await;
            }
            RequestType::Read => {
                info!("Read data");
                let res = self.read().await;
                let mut extra = Map::new();
                extra.insert(String::from("value"), json!(res));
                return runtime.reply(req, MessageBody::from_extra(extra)).await;
            }
            RequestType::Full { state } => {
                info!("Gossip replicate full state");
                self.replace_state(&req.src, state).await;
                return runtime.reply_ok(req).await;
            }
            RequestType::Init => {
                info!("Init request received, start Gossipping");
                Self::run_gossip(
                    self.counter.clone(),
                    runtime.clone(),
                    runtime.node_id().to_string(),
                    neighbors,
                    Duration::from_millis(1000),
                );
                return runtime.reply_ok(req).await;
            }
            RequestType::Replicate {delta} => { 
                info!("Replication received");
                self.add(&req.src, delta).await;
                return runtime.reply_ok(req).await;
            }
        }
    }

    fn run_gossip(
        counter: Arc<RwLock<HashMap<String, i64>>>,
        runtime: Runtime,
        node_id: String,
        neighbors: Vec<String>,
        period: Duration,
    ) {
        tokio::spawn(async move {
            loop {
                let counter_reader = counter.read().await;

                let rnd = rand::random_range(0..neighbors.len());
                let partner = neighbors.get(rnd).unwrap();
                let mut extra = Map::new();
                extra.insert(
                    String::from("state"),
                    json!(counter_reader.get(&node_id).cloned().unwrap_or(0)),
                );

                let msg_body = MessageBody::from_extra(extra).with_type("full");
                runtime.call_async(partner, msg_body);

                time::sleep(period).await
            }
        });
    }

    async fn add(&self, node_id: &str, delta: i64) {
        let mut counter_writer = self.counter.write().await;
        let curr_count = counter_writer.get(node_id).cloned();
        if curr_count.is_none() {
            counter_writer.insert(node_id.to_string(), delta);
            return;
        }
        counter_writer.insert(node_id.to_string(), curr_count.unwrap() + delta);
    }

    async fn replicate(&self, runtime: &Runtime, neighbors: Vec<String>, delta: i64) {
        for neighbor in neighbors {
            let mut extra = Map::new();
            extra.insert("delta".to_string(), json!(delta));
            runtime.call_async(&neighbor, MessageBody::from_extra(extra).with_type("replicate"));
        }
    }

    async fn replace_state(&self, node_id: &str, state: i64) {
        let mut counter_writer = self.counter.write().await;
        let curr_state = counter_writer.get(node_id).cloned();
        counter_writer.insert(node_id.to_string(), max(curr_state.unwrap_or(0), state));
    }

    async fn read(&self) -> i64 {
        let counter_reader = self.counter.read().await;

        counter_reader
            .values()
            .cloned()
            .reduce(|sum, entry| sum + entry)
            .or(Some(0))
            .unwrap()
    }
}

enum RequestType {
    Init,
    Add { delta: i64 },
    Read,
    Full { state: i64 },
    Replicate { delta: i64},
}

impl RequestType {
    fn from_str(s: &str, metadata: &Map<String, Value>) -> Option<RequestType> {
        match s {
            "add" => {
                let delta = metadata
                    .get("delta")
                    .expect("missing 'delta' argument")
                    .as_i64()
                    .expect("'delta' is not i64");

                Some(RequestType::Add { delta: delta })
            }
            "read" => Some(RequestType::Read),
            "full" => {
                let state = metadata
                    .get("state")
                    .expect("missing 'state' argument")
                    .as_i64()
                    .expect("'state' not a i64");

                Some(RequestType::Full { state: state })
            }
            "replicate" => {
                let delta = metadata
                    .get("delta")
                    .expect("missing 'delta' argument")
                    .as_i64()
                    .expect("'delta' is not i64");

                Some(RequestType::Replicate { delta: delta })
            }
            "init" => Some(RequestType::Init),
            _ => None,
        }
    }
}
