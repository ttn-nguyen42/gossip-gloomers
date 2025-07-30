package raft

import (
	"encoding/json"
	"log"
	"math/rand"
	"sync"
	"time"

	ms "github.com/jepsen-io/maelstrom/demo/go"
)

const (
	Follower  = "follower"
	Candidate = "candidate"
	Leader    = "leader"
)

type LogEntry struct {
	Term    int
	Index   int
	Command any
}

type Raft struct {
	mu sync.Mutex

	n      *ms.Node
	peers  []string
	nodeId string

	leaderId string

	state       string
	currentTerm int
	votedFor    string

	log         []LogEntry
	commitIndex int
	lastApplied int

	nextIndex  map[string]int
	matchIndex map[string]int

	electionTimer  *time.Timer
	heartbeatTimer *time.Timer

	applyCh chan LogEntry
}

type RequestVoteRequest struct {
	Type         string `json:"type"`
	Term         int    `json:"term"`
	CandidateID  string `json:"candidate_id"`
	LastLogIndex int    `json:"last_log_index"`
	LastLogTerm  int    `json:"last_log_term"`
}

type RequestVoteReply struct {
	Type        string `json:"type"`
	Term        int    `json:"term"`
	VoteGranted bool   `json:"vote_granted"`
}

type AppendEntriesRequest struct {
	Type         string     `json:"type"`
	Term         int        `json:"term"`
	LeaderID     string     `json:"leader_id"`
	PrevLogIndex int        `json:"prev_log_index"`
	PrevLogTerm  int        `json:"prev_log_term"`
	Entries      []LogEntry `json:"entries"`
	LeaderCommit int        `json:"leader_commit"`
}

type AppendEntriesReply struct {
	Type    string `json:"type"`
	Term    int    `json:"term"`
	Success bool   `json:"success"`
}

func (rf *Raft) becomeFollower(term int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.state = Follower
	rf.currentTerm = term
	rf.votedFor = ""
	rf.leaderId = ""
	rf.electionTimer.Reset(rf.randomElectionTimeout())
}

func (rf *Raft) becomeCandidate() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.nodeId
	rf.leaderId = ""
	rf.electionTimer.Reset(rf.randomElectionTimeout())

	rf.startElection()
}

func (rf *Raft) becomeLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.state = Leader
	rf.leaderId = rf.nodeId
	rf.nextIndex = make(map[string]int)
	rf.matchIndex = make(map[string]int)
	for _, peer := range rf.peers {
		rf.nextIndex[peer] = len(rf.log) + 1
		rf.matchIndex[peer] = 0
	}

	rf.heartbeatTimer.Reset(100 * time.Millisecond)
}

func (rf *Raft) startElection() {
	voteCount := 1
	lastLogIndex := len(rf.log) - 1
	lastLogTerm := 0
	if lastLogIndex >= 0 {
		lastLogTerm = rf.log[lastLogIndex].Term
	}

	for _, peer := range rf.peers {
		if peer == rf.nodeId {
			continue
		}

		go func(peer string) {
			reqBody := RequestVoteRequest{
				Type:         "request_vote",
				Term:         rf.currentTerm,
				CandidateID:  rf.nodeId,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}

			rf.n.RPC(peer, reqBody, func(msg ms.Message) error {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				log.Printf("Received request vote reply from %s", msg.Src)

				var bodyType struct {
					Type string `json:"type"`
				}
				if err := json.Unmarshal(msg.Body, &bodyType); err != nil {
					return err
				}

				if rf.state != Candidate || bodyType.Type != "request_vote_ok" {
					return nil
				}

				var reply RequestVoteReply
				if err := json.Unmarshal(msg.Body, &reply); err != nil {
					return err
				}

				if reply.VoteGranted {
					voteCount += 1
					if voteCount > len(rf.peers)/2 {
						rf.becomeLeader()
					}
				} else if reply.Term > rf.currentTerm {
					rf.becomeFollower(reply.Term)
				}
				return nil
			})
		}(peer)
	}
}

func (rf *Raft) HandleRequestVote(msg ms.Message) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	log.Printf("Received request vote from %s", msg.Src)

	var req RequestVoteRequest
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		return err
	}

	reply := RequestVoteReply{Type: "request_vote_ok", Term: rf.currentTerm, VoteGranted: false}

	if req.Term < rf.currentTerm {
		return rf.n.Reply(msg, reply)
	}

	if req.Term > rf.currentTerm {
		rf.becomeFollower(req.Term)
	}

	if rf.votedFor == "" || rf.votedFor == req.CandidateID {
		rf.votedFor = req.CandidateID
		reply.VoteGranted = true
	}

	return rf.n.Reply(msg, reply)
}

func (rf *Raft) sendHeartbeats() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for _, peer := range rf.peers {
		if peer == rf.nodeId {
			continue
		}
		go rf.sendAppendEntries(peer)
	}
	rf.heartbeatTimer.Reset(100 * time.Millisecond)
}

func (rf *Raft) sendAppendEntries(peer string) {
	prevLogIndex := rf.nextIndex[peer] - 1
	prevLogTerm := 0
	if prevLogIndex > 0 {
		prevLogTerm = rf.log[prevLogIndex-1].Term
	}

	entries := rf.log[prevLogIndex:]

	reqBody := AppendEntriesRequest{
		Type:         "append_entries",
		Term:         rf.currentTerm,
		LeaderID:     rf.nodeId,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}

	rf.n.RPC(peer, reqBody, func(msg ms.Message) error {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		var bodyType struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(msg.Body, &bodyType); err != nil {
			return err
		}

		if rf.state != Leader || bodyType.Type != "append_entries_ok" {
			return nil
		}

		var reply AppendEntriesReply
		if err := json.Unmarshal(msg.Body, &reply); err != nil {
			return err
		}

		if reply.Success {
			rf.nextIndex[peer] = prevLogIndex + len(entries) + 1
			rf.matchIndex[peer] = rf.nextIndex[peer] - 1
			rf.updateCommitIndex()
		} else if reply.Term > rf.currentTerm {
			rf.becomeFollower(reply.Term)
		} else {
			rf.nextIndex[peer] -= 1
		}
		return nil
	})
}

func (rf *Raft) HandleAppendEntries(msg ms.Message) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var req AppendEntriesRequest
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		return err
	}

	reply := AppendEntriesReply{Type: "append_entries_ok", Term: rf.currentTerm, Success: false}

	if req.Term < rf.currentTerm {
		return rf.n.Reply(msg, reply)
	}

	rf.becomeFollower(req.Term)
	rf.leaderId = req.LeaderID

	if req.PrevLogIndex > 0 && (len(rf.log) < req.PrevLogIndex || rf.log[req.PrevLogIndex-1].Term != req.PrevLogTerm) {
		return rf.n.Reply(msg, reply)
	}

	for i, entry := range req.Entries {
		index := req.PrevLogIndex + 1 + i
		if index > len(rf.log) {
			rf.log = append(rf.log, entry)
		} else {
			rf.log[index-1] = entry
		}
	}

	if req.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(req.LeaderCommit, len(rf.log))
		rf.applyCommitted()
	}

	reply.Success = true
	return rf.n.Reply(msg, reply)
}

func (rf *Raft) updateCommitIndex() {
	for n := len(rf.log); n > rf.commitIndex; n-- {
		if rf.log[n-1].Term == rf.currentTerm {
			count := 1
			for _, peer := range rf.peers {
				if rf.matchIndex[peer] >= n {
					count += 1
				}
			}
			if count > len(rf.peers)/2 {
				rf.commitIndex = n
				rf.applyCommitted()
				break
			}
		}
	}
}

func (rf *Raft) applyCommitted() {
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied += 1
		entry := rf.log[rf.lastApplied-1]
		rf.applyCh <- entry
	}
}

func (rf *Raft) Apply(command any) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return 0, 0, false
	}

	index := len(rf.log) + 1
	entry := LogEntry{Term: rf.currentTerm, Index: index, Command: command}
	rf.log = append(rf.log, entry)

	return index, rf.currentTerm, true
}

func (rf *Raft) IsLeader() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state == Leader
}

func (rf *Raft) GetLeader() string {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.leaderId
}

func (rf *Raft) randomElectionTimeout() time.Duration {
	return time.Duration(150+rand.Intn(150)) * time.Millisecond
}

func (rf *Raft) runTimers() {
	for {
		select {
		case <-rf.electionTimer.C:
			rf.becomeCandidate()
		case <-rf.heartbeatTimer.C:
			if rf.state == Leader {
				rf.sendHeartbeats()
			}
		}
	}
}

func NewRaft(n *ms.Node, nodeId string, peers []string, applyCh chan LogEntry) *Raft {
	rf := &Raft{
		n:              n,
		nodeId:         nodeId,
		peers:          peers,
		state:          Follower,
		currentTerm:    0,
		votedFor:       "",
		log:            make([]LogEntry, 0),
		commitIndex:    0,
		lastApplied:    0,
		electionTimer:  time.NewTimer(0),
		heartbeatTimer: time.NewTimer(time.Second),
		applyCh:        applyCh,
		leaderId:       "",
	}

	n.Handle("request_vote", rf.HandleRequestVote)
	n.Handle("append_entries", rf.HandleAppendEntries)

	rf.becomeFollower(0)
	go rf.runTimers()

	return rf
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
