use std::sync::Arc;

use async_trait::async_trait;
use maelstrom::{
    Node, Result, Runtime, done,
    protocol::{Message, MessageBody},
};
use serde_json::Value;
use uuid::Uuid;

fn main() -> Result<()> {
    Runtime::init(try_main())
}

async fn try_main() -> Result<()> {
    let handler = Arc::new(Handler::new());
    Runtime::new().with_handler(handler).run().await
}

#[derive(Clone, Default)]
struct Handler {
    generator: Generator,
}

#[async_trait]
impl Node for Handler {
    async fn process(&self, runtime: Runtime, req: Message) -> Result<()> {
        let gen_res = self.generator.generate(&req);
        if let Ok(msg_body) = gen_res {
            return runtime.reply(req, msg_body).await;
        }
        done(runtime, req)
    }
}

impl Handler {
    pub fn new() -> Handler {
        return Handler {
            generator: Generator {},
        };
    }
}

#[derive(Clone, Default)]
struct Generator {}

impl Generator {
    fn generate(&self, req: &Message) -> Result<MessageBody> {
        if req.get_type() != "generate" {
            return Err("Not generate message".into());
        }
        let mut body = req.body.clone().with_type("generate_ok");
        body.extra.insert(
            String::from("id"),
            Value::String(Uuid::new_v4().to_string()),
        );
        Ok(body)
    }
}
