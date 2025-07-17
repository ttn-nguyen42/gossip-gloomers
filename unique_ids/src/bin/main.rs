use std::{
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use crc32fast::Hasher;
use maelstrom::{
    Node, Result, Runtime, done,
    protocol::{Message, MessageBody},
};
use serde_json::Value;

fn main() -> Result<()> {
    Runtime::init(try_main())
}

async fn try_main() -> Result<()> {
    let runtime = Runtime::new();
    let handler = Arc::new(Handler::new(runtime.node_id()));
    runtime.with_handler(handler).run().await
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
    pub fn new(node_id: &str) -> Handler {
        let generator = Generator {
            guid: Guid::new(node_id),
        };
        return Handler {
            generator: generator,
        };
    }
}

#[derive(Clone, Default)]
struct Generator {
    guid: Guid,
}

impl Generator {
    fn generate(&self, req: &Message) -> Result<MessageBody> {
        if req.get_type() != "generate" {
            return Err("Not generate message".into());
        }
        let mut body = req.body.clone().with_type("generate_ok");
        let id = self.guid.next();
        body.extra.insert(String::from("id"), Value::String(id));
        Ok(body)
    }
}

#[derive(Clone, Default)]
struct Guid {
    counter: Arc<AtomicU32>,
    hashed_id: u64,
}

impl Guid {
    pub fn new(node_id: &str) -> Guid {
        return Guid {
            counter: Arc::new(AtomicU32::new(0)),
            hashed_id: Guid::hash_10_bits(node_id),
        };
    }

    pub fn next(&self) -> String {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Invalid current time, backward from UNIX_EPOCH?");
        let ts_u64 = ts.as_millis();
        let ts_41_bits = (ts_u64 & 0x1FFFFFFFFFF) as u64; // 41 bits
        let counter = self.counter.fetch_add(1, Ordering::SeqCst);
        let short_counter = counter as u64 & 0x7FF800; // 12 bits
        let id = (ts_41_bits << 23) | (short_counter << 11) | (self.hashed_id << 1);
        id.to_string()
    }

    fn hash_10_bits(node_id: &str) -> u64 {
        let mut hasher = Hasher::new();
        hasher.update(node_id.as_bytes());
        let hash = hasher.finalize();
        (hash & 0x3FF) as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_guid() {
        let guid = Guid::new("1");
        println!("{}", guid.next());
    }
}
