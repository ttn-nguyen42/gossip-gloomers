use crate::common::Operation;

#[derive(Clone, Copy)]
pub struct StateMachine {}

impl StateMachine {
    pub fn new() -> StateMachine {
        StateMachine {}
    }

    pub async fn apply(&self, ops: Vec<Operation>) -> Result<(), String> {
        Ok(())
    }
}
