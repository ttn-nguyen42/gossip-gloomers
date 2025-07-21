use crate::common::{Operation, Response};

#[derive(Clone, Copy)]
pub struct StateMachine {}

impl StateMachine {
    pub fn new() -> StateMachine {
        StateMachine {}
    }

    pub async fn apply(&self, ops: Vec<Operation>) -> Result<Response, String> {
        Ok(Response::TransactOk { txn: ops.clone() })
    }
}
