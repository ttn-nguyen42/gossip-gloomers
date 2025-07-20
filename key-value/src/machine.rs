use async_trait::async_trait;
use maelstrom::{Node, Result, Runtime, done, protocol::Message};

use crate::{cluster::Machine, common::Request};

pub struct Handler {}

impl Handler {
    pub fn new() -> Handler {
        Handler {}
    }
}

#[async_trait]
impl Node for Handler {
    async fn process(&self, runtime: Runtime, msg: Message) -> Result<()> {
        let req = Request::new(&msg);
        match req {
            Request::Transact { txn } => todo!(),
            Request::Init => {
                let machine = Machine::new(runtime.clone());
            }
        }
        done(runtime, msg)
    }
}
