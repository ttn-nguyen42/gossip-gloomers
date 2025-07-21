use std::{collections::HashMap, sync::Arc};
use log::info;
use tokio::sync::RwLock;

use crate::common::{Operation, Response};

#[derive(Clone)]
pub struct StateMachine {
    store: Arc<RwLock<HashMap<i64, i64>>>,
}

impl StateMachine {
    pub fn new() -> StateMachine {
        StateMachine {
            store: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn apply(&self, ops: Vec<Operation>) -> Result<Response, String> {
        let is_read_only = ops.iter().all(|op| matches!(op, Operation::Read { .. }));
        if is_read_only {
            self.apply_read_only(ops).await
        } else {
            self.apply_mixed(ops).await
        }
    }

    pub async fn apply_read_only(&self, ops: Vec<Operation>) -> Result<Response, String> {
        let store = self.store.read().await;
        let mut txn = Vec::new();
        for op in ops {
            match op {
                Operation::Read { key, result: _ } => {
                    if let Some(value) = store.get(&key) {
                        txn.push(Operation::Read {
                            key,
                            result: Some(*value),
                        });
                    } else {
                        info!("Read operation for key {} not found", key);
                        txn.push(Operation::Read { key, result: None });
                    }
                }
                Operation::Write { key: _, value: _ } => {
                    return Err(format!("Write operation in read-only transaction"));
                }
            }
        }
        Ok(Response::TransactOk { txn })
    }

    pub async fn apply_mixed(&self, ops: Vec<Operation>) -> Result<Response, String> {
        let mut store = self.store.write().await;
        let mut txn = Vec::new();
        for op in ops {
            match op {
                Operation::Read { key, result: _ } => {
                    if let Some(value) = store.get(&key) {
                        txn.push(Operation::Read {
                            key,
                            result: Some(*value),
                        });
                    } else {
                        info!("Read operation for key {} not found", key);
                        txn.push(Operation::Read { key, result: None });
                    }
                }
                Operation::Write { key, value } => {
                    store.insert(key, value);
                    txn.push(Operation::Write { key, value });
                }
            }
        }
        Ok(Response::TransactOk { txn })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_new_state_machine() {
        let sm = StateMachine::new();
        let store = sm.store.read().await;
        assert!(store.is_empty());
    }

    #[tokio::test]
    async fn test_read_only_empty_store() {
        let sm = StateMachine::new();
        let ops = vec![
            Operation::Read {
                key: 1,
                result: None,
            },
            Operation::Read {
                key: 2,
                result: None,
            },
        ];

        let result = sm.apply(ops).await;
        assert!(result.is_ok());

        let response = result.unwrap();
        match response {
            Response::TransactOk { txn } => {
                assert_eq!(txn.len(), 2);
                assert!(matches!(
                    txn[0],
                    Operation::Read {
                        key: 1,
                        result: None
                    }
                ));
                assert!(matches!(
                    txn[1],
                    Operation::Read {
                        key: 2,
                        result: None
                    }
                ));
            }
            _ => panic!("Expected TransactOk response"),
        }
    }

    #[tokio::test]
    async fn test_read_only_with_data() {
        let sm = StateMachine::new();

        // First, write some data
        let write_ops = vec![
            Operation::Write { key: 1, value: 100 },
            Operation::Write { key: 2, value: 200 },
        ];
        let _ = sm.apply_mixed(write_ops).await;

        // Then read the data
        let read_ops = vec![
            Operation::Read {
                key: 1,
                result: None,
            },
            Operation::Read {
                key: 2,
                result: None,
            },
            Operation::Read {
                key: 3,
                result: None,
            }, // non-existent key
        ];

        let result = sm.apply(read_ops).await;
        assert!(result.is_ok());

        let response = result.unwrap();
        match response {
            Response::TransactOk { txn } => {
                assert_eq!(txn.len(), 3);

                // Check first read
                match &txn[0] {
                    Operation::Read { key, result } => {
                        assert_eq!(*key, 1);
                        assert_eq!(*result, Some(100));
                    }
                    _ => panic!("Expected Read operation"),
                }

                // Check second read
                match &txn[1] {
                    Operation::Read { key, result } => {
                        assert_eq!(*key, 2);
                        assert_eq!(*result, Some(200));
                    }
                    _ => panic!("Expected Read operation"),
                }

                // Check third read (non-existent key)
                match &txn[2] {
                    Operation::Read { key, result } => {
                        assert_eq!(*key, 3);
                        assert_eq!(*result, None);
                    }
                    _ => panic!("Expected Read operation"),
                }
            }
            _ => panic!("Expected TransactOk response"),
        }
    }

    #[tokio::test]
    async fn test_mixed_operations() {
        let sm = StateMachine::new();

        let ops = vec![
            Operation::Write { key: 1, value: 100 },
            Operation::Read {
                key: 1,
                result: None,
            },
            Operation::Write { key: 2, value: 200 },
            Operation::Read {
                key: 2,
                result: None,
            },
        ];

        let result = sm.apply(ops).await;
        assert!(result.is_ok());

        let response = result.unwrap();
        match response {
            Response::TransactOk { txn } => {
                assert_eq!(txn.len(), 4);

                // Check write operations
                match &txn[0] {
                    Operation::Write { key, value } => {
                        assert_eq!(*key, 1);
                        assert_eq!(*value, 100);
                    }
                    _ => panic!("Expected Write operation"),
                }

                match &txn[2] {
                    Operation::Write { key, value } => {
                        assert_eq!(*key, 2);
                        assert_eq!(*value, 200);
                    }
                    _ => panic!("Expected Write operation"),
                }

                // Check read operations
                match &txn[1] {
                    Operation::Read { key, result } => {
                        assert_eq!(*key, 1);
                        assert_eq!(*result, Some(100));
                    }
                    _ => panic!("Expected Read operation"),
                }

                match &txn[3] {
                    Operation::Read { key, result } => {
                        assert_eq!(*key, 2);
                        assert_eq!(*result, Some(200));
                    }
                    _ => panic!("Expected Read operation"),
                }
            }
            _ => panic!("Expected TransactOk response"),
        }
    }

    #[tokio::test]
    async fn test_write_operation_in_read_only() {
        let sm = StateMachine::new();

        let ops = vec![
            Operation::Read {
                key: 1,
                result: None,
            },
            Operation::Write { key: 1, value: 100 }, // This should cause an error
        ];

        let result = sm.apply_read_only(ops).await;
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            "Write operation in read-only transaction"
        );
    }

    #[tokio::test]
    async fn test_overwrite_existing_key() {
        let sm = StateMachine::new();

        // First write
        let ops1 = vec![Operation::Write { key: 1, value: 100 }];
        let _ = sm.apply(ops1).await;

        // Overwrite the same key
        let ops2 = vec![Operation::Write { key: 1, value: 200 }];
        let _ = sm.apply(ops2).await;

        // Read to verify overwrite
        let read_ops = vec![Operation::Read {
            key: 1,
            result: None,
        }];
        let result = sm.apply(read_ops).await;
        assert!(result.is_ok());

        let response = result.unwrap();
        match response {
            Response::TransactOk { txn } => {
                assert_eq!(txn.len(), 1);
                match &txn[0] {
                    Operation::Read { key, result } => {
                        assert_eq!(*key, 1);
                        assert_eq!(*result, Some(200)); // Should be the new value
                    }
                    _ => panic!("Expected Read operation"),
                }
            }
            _ => panic!("Expected TransactOk response"),
        }
    }

    #[tokio::test]
    async fn test_empty_operations() {
        let sm = StateMachine::new();

        let ops = vec![];
        let result = sm.apply(ops).await;
        assert!(result.is_ok());

        let response = result.unwrap();
        match response {
            Response::TransactOk { txn } => {
                assert_eq!(txn.len(), 0);
            }
            _ => panic!("Expected TransactOk response"),
        }
    }

    #[tokio::test]
    async fn test_concurrent_access() {
        let sm = StateMachine::new();

        // Spawn multiple tasks to test concurrent access
        let handles: Vec<_> = (0..10)
            .map(|i| {
                let sm_clone = sm.clone();
                tokio::spawn(async move {
                    let ops = vec![
                        Operation::Write {
                            key: i,
                            value: i * 100,
                        },
                        Operation::Read {
                            key: i,
                            result: None,
                        },
                    ];
                    sm_clone.apply(ops).await
                })
            })
            .collect();

        // Wait for all tasks to complete
        for handle in handles {
            let result = handle.await.unwrap();
            assert!(result.is_ok());
        }

        // Verify all data was written correctly
        let read_ops: Vec<_> = (0..10)
            .map(|i| Operation::Read {
                key: i,
                result: None,
            })
            .collect();
        let result = sm.apply(read_ops).await;
        assert!(result.is_ok());

        let response = result.unwrap();
        match response {
            Response::TransactOk { txn } => {
                assert_eq!(txn.len(), 10);
                for (i, op) in txn.iter().enumerate() {
                    match op {
                        Operation::Read { key, result } => {
                            assert_eq!(*key, i as i64);
                            assert_eq!(*result, Some(i as i64 * 100));
                        }
                        _ => panic!("Expected Read operation"),
                    }
                }
            }
            _ => panic!("Expected TransactOk response"),
        }
    }
}
