use std::sync::Arc;

use key_value::machine::{Handler};
use maelstrom::{Result, Runtime};

fn main() -> Result<()> {
    Runtime::init(try_main())
}

async fn try_main() -> Result<()> {
    let machine = Handler::new();
    Runtime::new().with_handler(Arc::new(machine)).run().await
}
