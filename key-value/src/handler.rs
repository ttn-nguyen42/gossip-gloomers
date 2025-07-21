use std::sync::Arc;

use async_trait::async_trait;
use maelstrom::{Node, Result, Runtime, done, protocol::Message};
use tokio::sync::Mutex;

use crate::{cluster::Cluster, common::Request, state::StateMachine};

pub struct Handler {
    cluster: Arc<Mutex<Option<Cluster>>>,
}

impl Handler {
    pub fn new() -> Handler {
        Handler {
            cluster: Arc::new(Mutex::new(None)),
        }
    }
}

#[async_trait]
impl Node for Handler {
    async fn process(&self, runtime: Runtime, msg: Message) -> Result<()> {
        let req = Request::new(&msg);
        match req {
            Request::Transact { txn } => todo!(),
            Request::Init => {
                let mut cluster_guard = self.cluster.lock().await;
                if cluster_guard.is_none() {
                    let state_machine = StateMachine::new();
                    let mut cluster =
                        Cluster::new(runtime.clone(), "DATA".to_string(), state_machine);
                    cluster.start().await;
                    *cluster_guard = Some(cluster);
                }
            }
            Request::Cluster { body } => {
                let cluster_guard = self.cluster.lock().await;
                if let Some(cluster) = cluster_guard.as_ref() {
                    let res = cluster.handle_cluster(Request::Cluster { body }).await;
                    if let Err(err) = res {
                        return Err(err.into());
                    }
                    let response = res.expect("result is not of type Response");
                    let body = response.as_body();
                    let res = runtime.reply(msg.clone(), body).await;
                    if let Err(err) = res {
                        return Err(err.into());
                    }
                }
            }
        }
        done(runtime, msg)
    }
}
