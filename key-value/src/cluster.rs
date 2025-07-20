use bincode;
use log::info;
use maelstrom::Runtime;
use rand::Rng;
use std::collections::HashMap;
use std::io::{Error, Read};
use std::sync::Arc;
use std::time;
use std::{cmp::max, time::Duration};
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, SeekFrom};
use tokio::spawn;
use tokio::sync::Mutex;
use tokio::time::Instant;

use crate::common::{Operation, Request, Response};

const PAGE_SIZE: usize = 4096;
const ENTRY_HEADER: usize = 16;
const ENTRY_SIZE: usize = 128;

#[derive(Clone)]
pub struct Cluster {
    runtime: Runtime,
    meta_dir: String,
    inner: Arc<Mutex<Inner>>,
}

pub struct Inner {
    members: Vec<String>,
    node_id: String,

    heartbeat_dur: Duration,
    elect_timeout: Duration,
    heartbeat_timeout: Duration,
    machine: Machine,

    cur_term: u64,
    commit_index: u64,
    last_applied: u64,
    state: State,
    cluster_idx: u64,
    log: Vec<Entry>,

    stopped: bool,
    election_timeout: Instant,

    persist: Option<Persistence>,
    votes: HashMap<String, Vote>,
}

#[derive(Clone, Copy)]
enum State {
    Leader,
    Follower,
    Candidate,
}

#[derive(Clone)]
enum Vote {
    For { node_id: String },
    NotYet,
}

impl Vote {
    pub fn is_self(&self, us: String) -> bool {
        match self {
            Vote::For { node_id } => us.eq(node_id),
            Vote::NotYet => false,
        }
    }
}

impl Cluster {
    pub fn new(runtime: Runtime, meta_dir: String) -> Cluster {
        let members: Vec<String> = runtime.nodes().iter().cloned().collect();
        let node_id = runtime.node_id().to_string();
        let mut votes = HashMap::new();
        for m in &members {
            votes.insert(m.clone(), Vote::NotYet);
        }
        let inner = Inner {
            members: members,
            node_id: node_id,
            heartbeat_dur: Duration::from_millis(300),
            elect_timeout: Duration::from_millis(2000),
            heartbeat_timeout: Duration::from_millis(400),
            machine: Machine {
                runtime: runtime.clone(),
            },
            state: State::Candidate,
            cluster_idx: 0,
            last_applied: 0,
            commit_index: 0,
            log: Vec::new(),
            cur_term: 0,
            stopped: true,
            election_timeout: Instant::now(),
            persist: None,
            votes: votes,
        };
        Cluster {
            runtime: runtime.clone(),
            meta_dir: meta_dir,
            inner: Arc::new(Mutex::new(inner)),
        }
    }

    pub async fn start(&mut self) {
        let mut inner_l = self.inner.lock().await;
        inner_l.state = State::Candidate;
        inner_l.stopped = false;

        let deser_persist = Persistence::restore(self.meta_dir.as_str()).await;
        if let Err(err) = deser_persist {
            panic!("failed to deserialize persistence: {}", err);
        }
        inner_l.persist = Some(deser_persist.unwrap());

        let cluster_arc = Arc::new(self.clone());
        spawn(Cluster::run(cluster_arc));
    }

    async fn run(self: Arc<Self>) {
        let mut inner_l = self.inner.lock().await;
        inner_l.reset_elect_timeout().await;
        drop(inner_l);

        loop {
            let mut inner_l = self.inner.lock().await;

            if inner_l.stopped {
                return;
            }

            let state = inner_l.state;

            match state {
                State::Leader => {
                    inner_l.heartbeat().await;
                    inner_l.advance_commit_index().await;
                }
                State::Follower => {
                    inner_l.timeout().await;
                    inner_l.advance_commit_index().await;
                }
                State::Candidate => {
                    inner_l.timeout().await;
                    inner_l.become_leader().await;
                }
            }
        }
    }
}

impl Inner {
    async fn reset_elect_timeout(&mut self) {
        let mut rng = rand::rng();
        let heartbeat_ms = self.heartbeat_dur.as_millis() as u64;
        let interval = rng.random_range((heartbeat_ms * 2)..=(heartbeat_ms * 4));
        let interval_duration = Duration::from_millis(interval);

        self.election_timeout = Instant::now() + interval_duration;
    }

    async fn heartbeat(&mut self) {}

    async fn advance_commit_index(&mut self) {}

    async fn timeout(&mut self) {
        let now = Instant::now();
        let has_timed_out = now > self.election_timeout;

        if has_timed_out {
            info!("Timed out, starting electron");
            self.state = State::Candidate;
            self.cur_term += 1;

            for node_id in &self.members {
                if node_id.eq(&self.node_id) {
                    self.votes.insert(
                        node_id.clone(),
                        Vote::For {
                            node_id: self.node_id.clone(),
                        },
                    );
                } else {
                    self.votes.insert(node_id.clone(), Vote::NotYet);
                }
            }

            self.reset_elect_timeout().await;
            let persisted = self.persist.as_mut().unwrap().persist(false, 0).await;
            if let Err(err) = persisted {
                panic!("failed to persist cluster data: {}", err)
            }
            self.request_vote().await;
        }
    }

    async fn become_leader(&mut self) {}

    async fn request_vote(&mut self) {}
}

pub struct Machine {
    runtime: Runtime,
}

impl Machine {
    pub fn new(runtime: Runtime) -> Machine {
        Machine { runtime: runtime }
    }

    pub fn transact(&self, req: &Request) -> Response {
        Response::TransactOk { txn: Vec::new() }
    }
}

pub struct Entry {
    pub term: u64,
    pub operations: Vec<Operation>,
}

impl Entry {
    pub fn serialize(&self) -> [u8; ENTRY_SIZE] {
        let mut entry_bytes = [0u8; ENTRY_SIZE];

        // Serialize operations to bytes
        let operations_bytes = bincode::serialize(&self.operations).unwrap_or_else(|_| Vec::new());

        if operations_bytes.len() > ENTRY_SIZE - ENTRY_HEADER {
            panic!(
                "Operations are too large ({}). Must be at most {} bytes.",
                operations_bytes.len(),
                ENTRY_SIZE - ENTRY_HEADER
            );
        }

        // ----------------------------------------------------------------
        // | Entry term (8b) | Operations length (8b) | Operations (112b) |
        // ----------------------------------------------------------------
        entry_bytes[0..8].copy_from_slice(&self.term.to_le_bytes());
        entry_bytes[8..16].copy_from_slice(&(operations_bytes.len() as u64).to_le_bytes());
        entry_bytes[16..16 + operations_bytes.len()].copy_from_slice(&operations_bytes);

        entry_bytes
    }

    pub fn deserialize(entry_bytes: &[u8]) -> Result<Entry, String> {
        if entry_bytes.len() != ENTRY_SIZE {
            return Err(format!(
                "Invalid entry size: expected {}, got {}",
                ENTRY_SIZE,
                entry_bytes.len()
            ));
        }

        let term = u64::from_le_bytes(entry_bytes[0..8].try_into().unwrap());

        let operations_len = u64::from_le_bytes(entry_bytes[8..16].try_into().unwrap()) as usize;

        let operations_bytes = &entry_bytes[16..16 + operations_len];
        let operations = bincode::deserialize(operations_bytes)
            .map_err(|e| format!("Failed to deserialize operations: {}", e))?;

        Ok(Entry { term, operations })
    }
}

pub struct EntryResult {
    key: i64,
    val: i64,
}

impl EntryResult {
    pub fn new(key: i64, val: i64) -> EntryResult {
        EntryResult { key: key, val: val }
    }
}

pub struct Persistence {
    fd: File,
    current_term: u64,
    log_size: usize,
    log: Vec<Entry>,
    voted_for: u64,
}

impl Persistence {
    pub async fn open_metadata(meta_dir: &str) -> File {
        let metadata_path = format!("{}/metadata.dat", meta_dir);
        OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(metadata_path)
            .await
            .expect("Failed to open metadata file")
    }

    pub async fn persist(
        &mut self,
        write_log: bool,
        n_new_entries: usize,
    ) -> Result<(), std::io::Error> {
        let mut n_new_entries = n_new_entries;

        if n_new_entries == 0 && write_log {
            n_new_entries = self.log.len();
        }

        self.fd.seek(SeekFrom::Start(0)).await?;

        let mut page = [0u8; PAGE_SIZE];
        // --------------------------------------------------------
        // | Current term (8b) | Voted for (8b) | Log length (8b) |
        // --------------------------------------------------------
        page[0..8].copy_from_slice(&self.current_term.to_le_bytes());
        page[8..16].copy_from_slice(&self.voted_for.to_le_bytes());
        page[16..24].copy_from_slice(&(self.log.len() as u64).to_le_bytes());

        self.fd.write_all(&page).await?;

        if write_log && n_new_entries > 0 {
            let new_log_offset = max(self.log.len() - n_new_entries, 0);

            self.fd
                .seek(SeekFrom::Start(
                    (PAGE_SIZE + ENTRY_SIZE * new_log_offset) as u64,
                ))
                .await?;

            for i in new_log_offset..self.log.len() {
                let entry = &self.log[i];
                let entry_bytes = entry.serialize();
                self.fd.write_all(&entry_bytes).await?;
            }
        }

        self.fd.sync_all().await?;
        Ok(())
    }

    pub async fn restore(path: &str) -> Result<Persistence, Error> {
        let mut fd = Persistence::open_metadata(path).await;

        fd.seek(SeekFrom::Start(0)).await?;

        // --------------------------------------------------------
        // | Current term (8b) | Voted for (8b) | Log length (8b) |
        // --------------------------------------------------------
        let mut page = [0u8; PAGE_SIZE];
        let n = fd.read(&mut page).await?;

        if n == 0 {
            // EOF - return empty persistence
            return Ok(Persistence {
                fd,
                current_term: 0,
                log_size: 0,
                log: Vec::new(),
                voted_for: 0,
            });
        }

        if n != PAGE_SIZE {
            panic!("Read incomplete page: expected {}, got {}", PAGE_SIZE, n);
        }

        let current_term = u64::from_le_bytes(page[0..8].try_into().unwrap());
        let voted_for = u64::from_le_bytes(page[8..16].try_into().unwrap());
        let len_log = u64::from_le_bytes(page[16..24].try_into().unwrap());

        let mut log = Vec::new();

        if len_log > 0 {
            fd.seek(SeekFrom::Start(PAGE_SIZE as u64)).await?;

            for _ in 0..len_log {
                let mut entry_bytes = [0u8; ENTRY_SIZE];
                let n = fd.read(&mut entry_bytes).await?;
                if n != ENTRY_SIZE {
                    panic!("Read incomplete entry: expected {}, got {}", ENTRY_SIZE, n);
                }

                // ----------------------------------------------------------
                // | Entry term (8b) | Operations length (8b) | Operations (112b) |
                // ----------------------------------------------------------
                let entry = Entry::deserialize(&entry_bytes)
                    .unwrap_or_else(|e| panic!("Failed to deserialize entry: {}", e));

                log.push(entry);
            }
        }

        if len_log == 0 {
            log.push(Entry {
                term: 0,
                operations: Vec::new(),
            });
        }

        Ok(Persistence {
            fd,
            current_term,
            log_size: log.len(),
            log,
            voted_for,
        })
    }
}
