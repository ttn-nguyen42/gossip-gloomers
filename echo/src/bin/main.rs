use std::sync::Arc;

use async_trait::async_trait;
use maelstrom::{
    Node, Result, Runtime, done,
    protocol::{Message, MessageBody},
};

fn main() -> Result<()> {
    Runtime::init(try_main())
}

async fn try_main() -> Result<()> {
    let handler = Arc::new(Handler::default());
    Runtime::new().with_handler(handler).run().await
}

#[derive(Clone, Default)]
struct Handler {}

#[async_trait]
impl Node for Handler {
    async fn process(&self, runtime: Runtime, req: Message) -> Result<()> {
        done(
            runtime,
            Message {
                src: String::new(),
                dest: String::new(),
                body: MessageBody::default(),
            },
        )
    }
}
