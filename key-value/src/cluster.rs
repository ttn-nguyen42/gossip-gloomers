use bincode;
use log::{debug, info};
use maelstrom::Runtime;
use maelstrom::protocol::MessageBody;
use rand::Rng;
use serde::{Deserialize, Serialize};
use serde_json::{Map, json};
use std::cmp::min;
use std::collections::HashMap;
use std::fmt::{self, Debug};
use std::io::Error;
use std::sync::Arc;
use std::{cmp::max, time::Duration};
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, SeekFrom};
use tokio::spawn;
use tokio::sync::RwLock;
use tokio::sync::oneshot::{Sender, channel};
use tokio::time::Instant;
use tokio_context::context::Context;

use crate::common::{Operation, Request, Response};
use crate::state::StateMachine;

const PAGE_SIZE: usize = 4096;
const ENTRY_HEADER: usize = 16;
const ENTRY_SIZE: usize = 128;

#[derive(Clone)]
pub struct Cluster {
    meta_dir: String,
    inner: Arc<RwLock<Inner>>,
}

pub struct Inner {
    members: Vec<String>,
    node_id: String,
    state_machine: StateMachine,

    heartbeat_dur: Duration,
    election_timeout: Instant,
    heartbeat_timeout: Instant,
    rpc: RpcService,

    cur_term: u64,
    commit_index: u64,
    last_applied: u64,
    state: State,
    log: Vec<Entry>,

    stopped: bool,
    leader_id: Option<String>,

    persist: Option<Persistence>,
    votes: HashMap<String, Vote>,
    nodes: HashMap<String, ClusterState>,
}

struct ClusterState {
    id: String,
    // next log index to send to this node
    next_index: u64,
    // last log index replicated to this node
    match_index: u64,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum State {
    Leader,
    Follower,
    Candidate,
}

#[derive(Clone, PartialEq, Eq, Debug)]
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
    pub fn new(runtime: Runtime, meta_dir: String, state_machine: StateMachine) -> Cluster {
        let members: Vec<String> = runtime.nodes().iter().cloned().collect();
        let node_id = runtime.node_id().to_string();
        let mut votes = HashMap::new();
        for m in &members {
            votes.insert(m.clone(), Vote::NotYet);
        }
        let mut nodes = HashMap::new();
        for m in &members {
            nodes.insert(
                m.clone(),
                ClusterState {
                    id: m.clone(),
                    next_index: 0,
                    match_index: 0,
                },
            );
        }
        let inner = Inner {
            members: members,
            node_id: node_id,
            heartbeat_dur: Duration::from_millis(300),
            heartbeat_timeout: Instant::now(),
            rpc: RpcService::new(runtime.clone()),
            state: State::Candidate,
            last_applied: 0,
            commit_index: 0,
            log: Vec::new(),
            cur_term: 0,
            stopped: true,
            election_timeout: Instant::now(),
            persist: None,
            votes: votes,
            nodes: nodes,
            state_machine: state_machine,
            leader_id: None,
        };
        Cluster {
            meta_dir: meta_dir,
            inner: Arc::new(RwLock::new(inner)),
        }
    }

    pub async fn start(&mut self) {
        let mut inner_l = self.inner.write().await;
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
        let mut inner_l = self.inner.write().await;
        inner_l.reset_elect_timeout().await;
        drop(inner_l);

        loop {
            let mut inner_l = self.inner.write().await;

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

    pub async fn handle_cluster(&self, req: Request) -> Result<Response, String> {
        match req {
            Request::Cluster { body } => {
                let req = ClusterRequest::from_body(&body).map_err(|e| e.to_string())?;
                match req {
                    ClusterRequest::AskVote {
                        term,
                        candidate_id,
                        last_log_index,
                        last_log_term,
                    } => {
                        let res = self
                            .handle_vote_req(term, candidate_id, last_log_index, last_log_term)
                            .await;
                        if let Err(err) = res {
                            return Err(err);
                        }
                        let res = res.expect("handle_vote_req failed");
                        let body = res.to_body();
                        Ok(Response::Cluster { body })
                    }
                    ClusterRequest::AppendEntries {
                        term,
                        leader_id,
                        prev_log_index,
                        prev_log_term,
                        entries,
                        leader_commit,
                    } => {
                        let res = self
                            .handle_append_entries_req(
                                term,
                                leader_id,
                                prev_log_index,
                                prev_log_term,
                                entries,
                                leader_commit,
                            )
                            .await;
                        if let Err(err) = res {
                            return Err(err);
                        }
                        let res = res.expect("handle_append_entries_req failed");
                        let body = res.to_body();
                        Ok(Response::Cluster { body })
                    }
                }
            }
            _ => Err("not cluster's request".to_string()),
        }
    }

    async fn handle_append_entries_req(
        &self,
        term: u64,
        leader_id: String,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<Entry>,
        leader_commit: u64,
    ) -> Result<ClusterResponse, String> {
        let mut inner_l = self.inner.write().await;

        inner_l.update_term(term, leader_id.clone()).await;

        // Candidates (5.2) in Raft paper: If AppendEntries RPC received from new leader: convert to follower
        if term == inner_l.cur_term && inner_l.state == State::Candidate {
            inner_l.state = State::Follower;
        }

        inner_l.leader_id = Some(leader_id.clone());

        if inner_l.state != State::Follower {
            debug!(
                "Non follower node_id: {} received append entries request",
                inner_l.node_id
            );
            return Ok(ClusterResponse::AppendOk {
                term: term,
                success: false,
            });
        }

        if term < inner_l.cur_term {
            debug!("Term {} is stale, rejecting append entries request", term);
            return Ok(ClusterResponse::AppendOk {
                term: term,
                success: false,
            });
        }

        // valid leader, reset election timeout
        inner_l.reset_elect_timeout().await;

        if !self.is_log_index_valid(&inner_l, prev_log_index, prev_log_term) {
            return Ok(ClusterResponse::AppendOk {
                term: term,
                success: false,
            });
        }

        let n_new_entries = inner_l.add_to_log(prev_log_index, entries, leader_commit);

        let persisted = inner_l
            .persist
            .as_mut()
            .unwrap()
            .persist(n_new_entries != 0, n_new_entries)
            .await;
        if let Err(err) = persisted {
            panic!("failed to persist cluster data: {}", err);
        }

        Ok(ClusterResponse::AppendOk {
            term: term,
            success: true,
        })
    }

    fn is_log_index_valid(&self, inner_l: &Inner, prev_log_index: u64, prev_log_term: u64) -> bool {
        let log_len = inner_l.log.len() as u64;
        let valid_log_index = prev_log_index == 0
            || (prev_log_index < log_len
                && inner_l.log[prev_log_index as usize].term == prev_log_term);

        if !valid_log_index {
            debug!(
                "Invalid log index or term, rejecting append entries request, prev_log_index: {}, prev_log_term: {}, log_len: {}",
                prev_log_index, prev_log_term, log_len
            );
            return false;
        }
        return true;
    }

    async fn handle_vote_req(
        &self,
        term: u64,
        candidate_id: String,
        last_log_index: u64,
        last_log_term: u64,
    ) -> Result<ClusterResponse, String> {
        let mut inner_l = self.inner.write().await;
        inner_l.update_term(term, candidate_id.clone()).await;
        info!(
            "Received vote request from {} for term {}",
            candidate_id.clone(),
            term
        );
        let should_grant;
        let cur_term = inner_l.cur_term;
        if term < cur_term {
            info!(
                "Term {} is stale, rejecting vote, current term is {}",
                term, cur_term
            );
            return Ok(ClusterResponse::VoteOk {
                granted: false,
                term: cur_term,
            });
        }

        let cur_last_log_index = (inner_l.log.len() - 1) as u64;
        let cur_last_log_term = inner_l.log.last().map(|l| l.term).unwrap_or(0);

        let log_up_to_date = last_log_term > cur_last_log_term
            || (last_log_term == cur_last_log_term && last_log_index >= cur_last_log_index);

        debug!(
            "Handle vote candidate: {}, log_up_to_date: {}, our cur_last_log_index: {}, cur_last_log_term: {}",
            candidate_id, log_up_to_date, cur_last_log_index, cur_last_log_term
        );

        let cur_vote = inner_l.i_voted_for();
        let can_i_vote_for_candidate =
            cur_vote.is_self(candidate_id.clone()) || cur_vote == Vote::NotYet;

        debug!(
            "Handle vote candidate: {}, can_i_vote_for_candidate: {}, our cur_vote: {:?}",
            candidate_id, can_i_vote_for_candidate, cur_vote
        );

        let term_up_to_date = term == cur_term;
        debug!(
            "Handle vote candidate: {}, term_up_to_date: {}",
            candidate_id, term_up_to_date
        );

        should_grant = term_up_to_date && log_up_to_date && can_i_vote_for_candidate;

        if should_grant {
            debug!("Handle vote candidate: {}, granting vote", candidate_id);
            inner_l.votes.insert(
                candidate_id.clone(),
                Vote::For {
                    node_id: candidate_id.clone(),
                },
            );
            inner_l.reset_elect_timeout().await;
            let persisted = inner_l.persist.as_mut().unwrap().persist(false, 0).await;
            if let Err(err) = persisted {
                panic!("failed to persist cluster data: {}", err);
            }
        } else {
            debug!(
                "Handle vote candidate: {}, not granting vote, our cur_vote: {:?}",
                candidate_id, cur_vote
            );
        }

        Ok(ClusterResponse::VoteOk {
            granted: should_grant,
            term: cur_term,
        })
    }

    pub async fn apply(&self, ops: Vec<Operation>) -> Result<Response, String> {
        let mut inner_l = self.inner.write().await;
        let (is_leader, _) = inner_l.is_leader().await;
        if !is_leader {
            return Err("node is not leader".to_string());
        }

        info!("Replicating operations as log entry: {:?}", ops);

        let (tx, rx) = channel();
        let new_entry = Entry {
            term: inner_l.cur_term,
            operations: ops,
            result: Some(tx),
        };
        inner_l.log.push(new_entry);

        let persisted = inner_l.persist.as_mut().unwrap().persist(true, 1).await;
        if let Err(err) = persisted {
            panic!("failed to persist cluster data: {}", err);
        }

        debug!(
            "Waiting for commit index to advance, cur_term: {}",
            inner_l.cur_term
        );

        let _ = inner_l.append_entries().await;
        info!("Entry replicated, cur_term: {}", inner_l.cur_term);

        // prevent deadlock waiting for advance_commit_index to run
        drop(inner_l);

        let res = rx.await;
        if let Err(err) = res {
            return Err(err.to_string());
        }
        let res = res.unwrap();
        Ok(res.unwrap())
    }

    pub async fn is_leader(&self) -> (bool, String) {
        let inner_l = self.inner.read().await;
        inner_l.is_leader().await
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

    async fn heartbeat(&mut self) {
        let time_for_heartbeat = Instant::now() > self.heartbeat_timeout;
        if time_for_heartbeat {
            self.heartbeat_timeout = Instant::now() + self.heartbeat_dur;
            debug!("Sending heartbeat");
            let _ = self.append_entries().await;
        }
    }

    async fn advance_commit_index(&mut self) {
        // Leader can update commitIndex on quorum.
        if self.state == State::Leader {
            let last_log_index = (self.log.len() - 1) as u64;

            for i in (self.commit_index + 1)..=last_log_index {
                let mut quorum = (self.members.len() / 2) + 1;

                for (node_id, node_state) in &self.nodes {
                    if quorum == 0 {
                        break;
                    }

                    let is_leader = node_id == &self.node_id;
                    if node_state.match_index >= i || is_leader {
                        quorum -= 1;
                    }
                }

                if quorum == 0 {
                    self.commit_index = i;
                    debug!("New commit index: {}", i);
                    break;
                }
            }
        }

        // Apply committed entries
        while self.last_applied <= self.commit_index {
            let log_index = self.last_applied as usize;
            if log_index >= self.log.len() {
                break;
            }

            let log_entry = &mut self.log[log_index];

            // len(log.operations) == 0 is a noop committed by the leader.
            if !log_entry.operations.is_empty() {
                debug!("Entry applied: {}", self.last_applied);
                let res = self.state_machine.apply(log_entry.operations.clone()).await;
                let result = log_entry.result.take();

                if let Err(err) = res {
                    debug!("Failed to apply entry {}: {}", self.last_applied, err);
                    if let Some(result) = result {
                        let _ = result.send(Err(err.to_string()));
                    } else {
                        panic!("No result channel for entry {}", self.last_applied);
                    }
                } else {
                    if let Some(result) = result {
                        let _ = result.send(Ok(Response::TransactOk {
                            txn: log_entry.operations.clone(),
                        }));
                    } else {
                        panic!("No result channel for entry {}", self.last_applied);
                    }
                }
            }

            self.last_applied += 1;
        }
    }

    fn add_to_log(
        &mut self,
        prev_log_index: u64,
        entries: Vec<Entry>,
        leader_commit: u64,
    ) -> usize {
        let next = prev_log_index + 1;
        let mut n_new_entries = 0;

        for i in next..(next + entries.len() as u64) {
            let entry_index = (i - next) as usize;
            let entry = &entries[entry_index];

            // Ensure log has enough capacity
            if i >= self.log.len() as u64 {
                let new_total = next + entries.len() as u64;
                let mut new_log = Vec::with_capacity((new_total * 2) as usize);
                new_log.extend_from_slice(&self.log);
                self.log = new_log;
            }

            // If an existing entry conflicts with a new one (same index but different terms),
            // delete the existing entry and all that follow it (§5.3)
            if i < self.log.len() as u64 && self.log[i as usize].term != entry.term {
                self.log.truncate(i as usize);
            }

            debug!("Appending entry at index: {}", self.log.len());

            if i < self.log.len() as u64 {
                // Entry already exists and terms match
                debug_assert_eq!(self.log[i as usize].term, entry.term);
            } else {
                self.log.push(entry.clone());
                debug_assert_eq!(self.log.len() as u64, i + 1);
                n_new_entries += 1;
            }
        }

        // Update commit index
        if leader_commit > self.commit_index {
            self.commit_index = min(leader_commit, (self.log.len() - 1) as u64);
        }

        n_new_entries
    }

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

    async fn become_leader(&mut self) {
        let quorum = (self.members.len() / 2) + 1;
        let mut n_votes = 0;
        for (_, vote) in &self.votes {
            if vote.is_self(self.node_id.clone()) {
                n_votes += 1;
            }
        }
        if n_votes >= quorum {
            info!(
                "Becoming leader, current term: {}, node_id: {}",
                self.cur_term, self.node_id
            );
            self.state = State::Leader;
            for (_, state) in &mut self.nodes {
                state.next_index = (self.log.len() + 1) as u64;
                state.match_index = 0;
                debug!("Reset next_index and match_index for node: {}", state.id);
            }
            // not really sure why we need to push an empty entry here
            // but it's what the paper says
            self.log.push(Entry {
                term: self.cur_term,
                operations: Vec::new(),
                result: None,
            });
            self.persist
                .as_mut()
                .unwrap()
                .persist(true, 1)
                .await
                .expect("persist failed");

            // start sending heartbeats right away
            self.heartbeat_timeout = Instant::now();
        }
    }

    async fn append_entries(&mut self) -> Result<HashMap<String, ClusterResponse>, String> {
        let mut resps = HashMap::new();
        for node_id in self.members.clone() {
            if self.node_id.eq(&node_id) {
                continue;
            }

            let next_index = self.nodes.get(&node_id).unwrap().next_index;
            let prev_log_index = next_index - 1;
            let prev_log_term = self.log.get(prev_log_index as usize).unwrap().term;

            let entries;
            if self.log.len() as u64 > next_index {
                debug!(
                    "Append entries len: {} next_index: {} node_id: {}",
                    self.log.len(),
                    next_index,
                    node_id
                );
                entries = self.log[next_index as usize..].to_vec();
            } else {
                entries = Vec::new();
            }

            let entries_len = entries.len();

            let req = ClusterRequest::AppendEntries {
                term: self.cur_term,
                leader_id: self.node_id.clone(),
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit: self.commit_index,
            };

            debug!("Sending append entries request to {}: {:?}", node_id, req);
            let res = self.rpc.send(node_id.clone(), req).await;
            if let Err(err) = res {
                info!(
                    "failed to send append entries request to {}: {}",
                    node_id, err
                );
                continue;
            }

            let resp = res.unwrap();
            match resp {
                ClusterResponse::AppendOk { term, success } => {
                    info!("received append ok response from {}: {:?}", node_id, resp);

                    let node_id_clone = node_id.clone();
                    if self.update_term(term, node_id_clone).await {
                        continue;
                    }

                    let stale = term < self.cur_term && self.state != State::Leader;
                    if stale {
                        continue;
                    }

                    let prev_index = self.nodes.get_mut(&node_id).unwrap().next_index;
                    if success {
                        let next_index = max(prev_log_index + entries_len as u64 + 1, prev_index);
                        let match_index = next_index - 1;
                        debug!(
                            "Message accepted for {}, prev_index: {}, match_index: {}",
                            node_id, prev_index, match_index
                        );

                        self.nodes.get_mut(&node_id).unwrap().next_index = next_index;
                        self.nodes.get_mut(&node_id).unwrap().match_index = match_index;
                    } else {
                        let next_index = max(prev_index - 1, 1);
                        debug!(
                            "Forced to revert to prev_index: {} for node_id: {}",
                            next_index, node_id
                        );
                        self.nodes.get_mut(&node_id).unwrap().next_index = next_index;
                    }
                }
                _ => {
                    info!("received unexpected response from {}: {:?}", node_id, resp);
                }
            }
            resps.insert(node_id.clone(), resp);
        }

        Ok(resps)
    }

    async fn request_vote(&mut self) {
        let last_log_index = (self.log.len() - 1) as u64;
        let last_log_term = self.log.last().map(|l| l.term).unwrap_or(0);
        let req = ClusterRequest::AskVote {
            term: self.cur_term,
            candidate_id: self.node_id.clone(),
            last_log_index: last_log_index,
            last_log_term: last_log_term,
        };
        let res = self.rpc.multi_cast(self.members.clone(), req).await;
        if let Err(err) = res {
            info!("failed to request vote: {}", err);
            return;
        }
        let resp = res.expect("result is not of type HashMap");
        for (node_id, r) in resp {
            match r {
                ClusterResponse::VoteOk { granted, term } => {
                    if self.update_term(term, node_id.clone()).await {
                        continue;
                    }

                    let is_stale = term < self.cur_term;
                    if is_stale {
                        continue;
                    }

                    if granted {
                        self.votes.insert(
                            node_id.clone(),
                            Vote::For {
                                node_id: self.node_id.clone(),
                            },
                        );
                    }
                }
                _ => {
                    info!("received unexpected response from {}: {:?}", node_id, r);
                }
            };
        }
    }

    async fn update_term(&mut self, term: u64, node_id: String) -> bool {
        let mut transitioned = false;
        if term > self.cur_term {
            self.cur_term = term;
            self.state = State::Follower;
            self.votes.insert(self.node_id.clone(), Vote::NotYet);
            transitioned = true;
            info!(
                "New term {} from {}, transitioning to follower",
                term, node_id
            );
            self.reset_elect_timeout().await;
            self.persist
                .as_mut()
                .unwrap()
                .persist(false, 0)
                .await
                .expect("persist failed");
        }
        transitioned
    }

    fn i_voted_for(&self) -> Vote {
        return self
            .votes
            .get(&self.node_id)
            .unwrap_or(&Vote::NotYet)
            .clone();
    }

    async fn is_leader(&self) -> (bool, String) {
        if self.state == State::Leader {
            (true, self.node_id.clone())
        } else {
            let leader_id = self.leader_id.clone().unwrap_or_default();
            (false, leader_id)
        }
    }
}

#[derive(Clone)]
pub struct RpcService {
    runtime: Runtime,
    node_id: String,
}

impl RpcService {
    pub fn new(runtime: Runtime) -> RpcService {
        RpcService {
            runtime: runtime.clone(),
            node_id: runtime.node_id().to_string(),
        }
    }

    async fn multi_cast(
        &self,
        node_ids: Vec<String>,
        req: ClusterRequest,
    ) -> Result<HashMap<String, ClusterResponse>, String> {
        let mut handles = Vec::new();
        for node_id in node_ids {
            if node_id == self.node_id {
                continue;
            }
            let self_clone = self.clone();
            let req_clone = req.clone();
            handles.push(spawn(async move {
                (node_id.clone(), self_clone.send(node_id, req_clone).await)
            }));
        }

        let mut responses = HashMap::new();
        for handle in handles {
            let res = handle.await;
            if let Err(err) = res {
                info!("failed to join on handle: {}", err);
                continue;
            }

            let (node_id, result) = res.unwrap();
            if let Ok(resp) = result {
                responses.insert(node_id, resp);
            }
        }

        Ok(responses)
    }

    async fn send(&self, node_id: String, req: ClusterRequest) -> Result<ClusterResponse, String> {
        let body = req.to_body();
        let (ctx, ctx_hdler) = Context::new();
        let res = self
            .runtime
            .call(ctx, node_id.clone(), body)
            .await
            .map_err(|e| e.to_string());

        if let Err(err) = res {
            ctx_hdler.cancel();
            return Err(format!("failed to send message to {}: {}", node_id, err));
        }

        ctx_hdler.cancel();
        ClusterResponse::from_body(&res.unwrap().body)
    }
}

#[derive(Serialize, Deserialize)]
pub struct Entry {
    pub term: u64,
    pub operations: Vec<Operation>,

    #[serde(skip)]
    pub result: Option<Sender<Result<Response, String>>>,
}

impl Clone for Entry {
    fn clone(&self) -> Self {
        Entry {
            term: self.term,
            operations: self.operations.clone(),
            result: None,
        }
    }
}

impl Debug for Entry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Entry {{ term: {}, operations: {:?} }}",
            self.term, self.operations
        )
    }
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

    pub fn deserialize(entry_bytes: &[u8]) -> std::result::Result<Entry, String> {
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

        Ok(Entry {
            term,
            operations,
            result: None,
        })
    }
}

pub struct Persistence {
    fd: File,
    current_term: u64,
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
    ) -> std::result::Result<(), std::io::Error> {
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

    pub async fn restore(path: &str) -> std::result::Result<Persistence, Error> {
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

                // ----------------------------------------------------------------
                // | Entry term (8b) | Operations length (8b) | Operations (112b) |
                // ----------------------------------------------------------------
                let entry = Entry::deserialize(&entry_bytes)
                    .unwrap_or_else(|e| panic!("Failed to deserialize entry: {}", e));

                log.push(entry);
            }
        }

        if len_log == 0 {
            log.push(Entry {
                term: 0,
                operations: Vec::new(),
                result: None,
            });
        }

        Ok(Persistence {
            fd,
            current_term,
            log,
            voted_for,
        })
    }
}

#[derive(Clone, Debug)]
enum ClusterRequest {
    AskVote {
        term: u64,
        candidate_id: String,
        last_log_index: u64,
        last_log_term: u64,
    },
    AppendEntries {
        term: u64,
        leader_id: String,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<Entry>,
        leader_commit: u64,
    },
}

impl ClusterRequest {
    pub fn to_body(&self) -> MessageBody {
        let mut extra = Map::new();
        let typ = match self {
            ClusterRequest::AskVote {
                term,
                candidate_id,
                last_log_index,
                last_log_term,
            } => {
                extra.insert("term".to_string(), json!(term));
                extra.insert("candidate_id".to_string(), json!(candidate_id));
                extra.insert("last_log_index".to_string(), json!(last_log_index));
                extra.insert("last_log_term".to_string(), json!(last_log_term));

                "ask_vote"
            }
            ClusterRequest::AppendEntries {
                term,
                leader_id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
            } => {
                extra.insert("term".to_string(), json!(term));
                extra.insert("leader_id".to_string(), json!(leader_id));
                extra.insert("prev_log_index".to_string(), json!(prev_log_index));
                extra.insert("prev_log_term".to_string(), json!(prev_log_term));
                extra.insert("entries".to_string(), json!(entries));
                extra.insert("leader_commit".to_string(), json!(leader_commit));
                "append_entries"
            }
        };
        MessageBody::from_extra(extra).with_type(typ)
    }

    pub fn from_body(body: &MessageBody) -> Result<Self, String> {
        let typ = &body.typ;
        match typ.as_str() {
            "ask_vote" => {
                let term = body
                    .extra
                    .get("term")
                    .and_then(|v| v.as_u64())
                    .ok_or_else(|| "missing 'term' field or not a u64".to_string())?;
                let candidate_id = body
                    .extra
                    .get("candidate_id")
                    .and_then(|v| v.as_str())
                    .map(String::from)
                    .ok_or_else(|| "missing 'candidate_id' field or not a string".to_string())?;
                let last_log_index = body
                    .extra
                    .get("last_log_index")
                    .and_then(|v| v.as_u64())
                    .ok_or_else(|| "missing 'last_log_index' field or not a u64".to_string())?;
                let last_log_term = body
                    .extra
                    .get("last_log_term")
                    .and_then(|v| v.as_u64())
                    .ok_or_else(|| "missing 'last_log_term' field or not a u64".to_string())?;

                Ok(ClusterRequest::AskVote {
                    term,
                    candidate_id,
                    last_log_index,
                    last_log_term,
                })
            }
            "append_entries" => {
                let term = body
                    .extra
                    .get("term")
                    .and_then(|v| v.as_u64())
                    .ok_or_else(|| "missing 'term' field or not a u64".to_string())?;
                let leader_id = body
                    .extra
                    .get("leader_id")
                    .and_then(|v| v.as_str())
                    .map(String::from)
                    .ok_or_else(|| "missing 'leader_id' field or not a string".to_string())?;
                let prev_log_index = body
                    .extra
                    .get("prev_log_index")
                    .and_then(|v| v.as_u64())
                    .ok_or_else(|| "missing 'prev_log_index' field or not a u64".to_string())?;
                let prev_log_term = body
                    .extra
                    .get("prev_log_term")
                    .and_then(|v| v.as_u64())
                    .ok_or_else(|| "missing 'prev_log_term' field or not a u64".to_string())?;
                let entries = body
                    .extra
                    .get("entries")
                    .and_then(|v| v.as_array())
                    .ok_or_else(|| "missing 'entries' field or not an array".to_string())?
                    .iter()
                    .map(|v| {
                        // Deserialize Entry from JSON
                        serde_json::from_value(v.clone())
                            .map_err(|e| format!("Failed to deserialize entry: {}", e))
                    })
                    .collect::<Result<Vec<Entry>, String>>()?;
                let leader_commit = body
                    .extra
                    .get("leader_commit")
                    .and_then(|v| v.as_u64())
                    .ok_or_else(|| "missing 'leader_commit' field or not a u64".to_string())?;
                Ok(ClusterRequest::AppendEntries {
                    term,
                    leader_id,
                    prev_log_index,
                    prev_log_term,
                    entries,
                    leader_commit,
                })
            }
            _ => Err(format!("Unknown cluster request type: {}", typ)),
        }
    }
}

#[derive(Debug)]
enum ClusterResponse {
    VoteOk { granted: bool, term: u64 },
    AppendOk { term: u64, success: bool },
}

impl ClusterResponse {
    pub fn to_body(&self) -> MessageBody {
        let mut extra = Map::new();
        let typ = match self {
            ClusterResponse::VoteOk { granted, term } => {
                extra.insert("granted".to_string(), json!(granted));
                extra.insert("term".to_string(), json!(term));
                "vote_ok"
            }
            ClusterResponse::AppendOk { term, success } => {
                extra.insert("term".to_string(), json!(term));
                extra.insert("success".to_string(), json!(success));
                "append_ok"
            }
        };
        MessageBody::from_extra(extra).with_type(typ)
    }

    pub fn from_body(body: &MessageBody) -> Result<Self, String> {
        let typ = &body.typ;

        match typ.as_str() {
            "vote_ok" => {
                let granted = body
                    .extra
                    .get("granted")
                    .and_then(|v| v.as_bool())
                    .ok_or_else(|| "missing 'granted' field or not a bool".to_string())?;
                let term = body
                    .extra
                    .get("term")
                    .and_then(|v| v.as_u64())
                    .ok_or_else(|| "missing 'term' field or not a u64".to_string())?;
                Ok(ClusterResponse::VoteOk { granted, term })
            }
            _ => Err(format!("Unknown cluster response type: {}", typ)),
        }
    }
}
