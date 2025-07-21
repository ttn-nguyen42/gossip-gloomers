use std::sync::Arc;

use async_trait::async_trait;
use log::info;
use maelstrom::{Node, Result, Runtime, protocol::Message};
use tokio::sync::Mutex;
use tokio_context::context::Context;

use crate::{
    cluster::Cluster,
    common::{Operation, Request, Response},
    state::StateMachine,
};

pub struct Handler {
    cluster: Arc<Mutex<Option<Cluster>>>,
    state_machine: Arc<Mutex<StateMachine>>,
}

impl Handler {
    pub fn new() -> Handler {
        Handler {
            cluster: Arc::new(Mutex::new(None)),
            state_machine: Arc::new(Mutex::new(StateMachine::new())),
        }
    }
}

#[async_trait]
impl Node for Handler {
    async fn process(&self, runtime: Runtime, msg: Message) -> Result<()> {
        let req = Request::new(&msg);
        match req {
            Request::Transact { txn } => {
                let cluster_guard = self.cluster.lock().await;

                async fn send_reply(
                    runtime: &Runtime,
                    msg: &Message,
                    resp: Response,
                ) -> Result<()> {
                    let body = resp.as_body();
                    let res = runtime.reply(msg.clone(), body).await;
                    if let Err(err) = res {
                        info!("Failed to send reply: {}", err);
                        return Err(err.into());
                    }
                    Ok(())
                }

                if let Some(cluster) = cluster_guard.as_ref() {
                    if self.has_write(&txn) {
                        let (is_leader, leader_id) = cluster.is_leader().await;
                        if is_leader {
                            let res = cluster.apply(txn).await;
                            if let Err(err) = res {
                                info!("Failed to apply: {}", err);
                                return Err(err.into());
                            }
                            let _ = send_reply(&runtime, &msg, res.unwrap()).await;
                        } else {
                            let res = self.forward_to_leader(&runtime, &msg, &leader_id).await;
                            if let Err(err) = res {
                                info!("Failed to forward to leader: {}", err);
                                return Err(err.into());
                            }
                            let _ = send_reply(&runtime, &msg, res.unwrap()).await;
                        }
                    } else {
                        let sm_guard = self.state_machine.lock().await;
                        let res = sm_guard.apply(txn).await;
                        if let Err(err) = res {
                            info!("Failed to apply: {}", err);
                            return Err(err.into());
                        }
                        let _ = send_reply(&runtime, &msg, res.unwrap()).await;
                    }
                }

                Ok(())
            }
            Request::Init => {
                let mut cluster_guard = self.cluster.lock().await;
                if cluster_guard.is_none() {
                    let mut cluster = Cluster::new(
                        runtime.clone(),
                        "./DATA".to_string(),
                        self.state_machine.clone(),
                    );
                    cluster.start().await;
                    *cluster_guard = Some(cluster);
                }
                let _ = runtime.reply_ok(msg.clone()).await;

                Ok(())
            }
            Request::Cluster { body } => {
                let cluster_guard = self.cluster.lock().await;
                if let Some(cluster) = cluster_guard.as_ref() {
                    let res = cluster.handle_cluster(Request::Cluster { body }).await;
                    if let Err(err) = res {
                        panic!("Failed to handle cluster: {}", err);
                    }
                    let response = res.expect("result is not of type Response");
                    let body = response.as_body();
                    let res = runtime.reply(msg.clone(), body).await;
                    if let Err(err) = res {
                        panic!("Failed to handle cluster: {}", err);
                    }
                }

                Ok(())
            }
        }
    }
}

impl Handler {
    fn has_write(&self, txn: &Vec<Operation>) -> bool {
        for op in txn {
            if let Operation::Write { key: _, value: _ } = op {
                return true;
            }
        }
        false
    }

    async fn forward_to_leader(
        &self,
        runtime: &Runtime,
        msg: &Message,
        leader_id: &str,
    ) -> Result<Response> {
        let (ctx, ctx_handler) = Context::new();
        let res = runtime.call(ctx, leader_id, msg.clone()).await;
        if let Err(err) = res {
            ctx_handler.cancel();
            return Err(err.into());
        }
        ctx_handler.cancel();
        Ok(Response::from_body(&res.unwrap().body).unwrap())
    }
}
