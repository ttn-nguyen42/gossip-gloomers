package server

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	ms "github.com/jepsen-io/maelstrom/demo/go"
	"github.com/ttn-nguyen42/key-value/internal/kv"
	"github.com/ttn-nguyen42/key-value/internal/raft"
)

type Server struct {
	mu         sync.Mutex
	n          *ms.Node
	store      *kv.Store
	raft       *raft.Raft
	pendingTxs map[int]chan *Tx
}

func NewServer(n *ms.Node) *Server {
	s := &Server{
		n:          n,
		store:      kv.NewStore(),
		pendingTxs: make(map[int]chan *Tx),
	}

	s.n.Handle("init", s.handleInit)
	s.n.Handle("txn", s.forwardTxn(s.handleTx))
	s.n.Handle("forward_txn", s.handleForwardTxn)
	return s
}

func (s *Server) handleInit(msg ms.Message) error {
	s.mu.Lock()
	log.Printf("Received init from %s", msg.Src)

	if s.raft != nil {
		s.mu.Unlock()
		return s.n.Reply(msg, NewInitOkReply())
	}

	applyCh := make(chan raft.LogEntry)
	s.raft = raft.NewRaft(s.n, s.n.ID(), s.n.NodeIDs(), applyCh)

	log.Printf("Starting raft")

	go func() {
		for entry := range applyCh {
			tx := entry.Command.(*Tx)
			applyTx(s.store, tx)

			s.mu.Lock()
			if ch, exists := s.pendingTxs[entry.Index]; exists {
				ch <- tx
				delete(s.pendingTxs, entry.Index)
			}
			s.mu.Unlock()
		}
	}()

	s.mu.Unlock()

	for s.raft.GetLeader() == "" {
		time.Sleep(100 * time.Millisecond)
	}

	return s.n.Reply(msg, NewInitOkReply())
}

func (s *Server) runTx(msg ms.Message) *Tx {
	tx := NewTx(msg)

	index, _, isLeader := s.raft.Apply(tx)
	if !isLeader {
		log.Fatalf("not leader: %s", msg.Body)
	}

	doneCh := make(chan *Tx)
	s.mu.Lock()
	s.pendingTxs[index] = doneCh
	s.mu.Unlock()

	select {
	case completedTx := <-doneCh:
		return completedTx
	case <-time.After(5 * time.Second):
		s.mu.Lock()
		delete(s.pendingTxs, index)
		s.mu.Unlock()
		log.Fatalf("timeout: %d", index)
	}
	return nil
}

func (s *Server) handleTx(msg ms.Message) error {
	reply := s.runTx(msg)
	return s.n.Reply(msg, NewTxOkReply(reply))
}

func (s *Server) handleForwardTxn(msg ms.Message) error {
	reply := s.runTx(msg)
	return s.n.Reply(msg, NewForwardTxnReply(reply))
}

func applyTx(store *kv.Store, txReq *Tx) {
	tx := store.Begin()
	defer tx.Commit()

	for _, op := range txReq.Ops {
		var err error
		switch op.Op {
		case "r":
			val, ok, err := tx.Get(op.Key)
			if err != nil {
				panic(err)
			}
			if ok {
				op.Value = val
			} else {
				log.Fatalf("key not found: %d", op.Key)
			}
		case "w":
			err = tx.Set(op.Key, op.Value)
			if err != nil {
				panic(err)
			}
		}
	}
}

func (s *Server) forwardTxn(handler func(msg ms.Message) error) func(msg ms.Message) error {
	return func(msg ms.Message) error {
		if s.raft.IsLeader() {
			log.Printf("I am leader, handling txn")
			return handler(msg)
		}

		leader := s.raft.GetLeader()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		body["type"] = "forward_txn"

		resp, err := s.n.SyncRPC(ctx, leader, body)
		if err != nil {
			return err
		}

		var reply Reply
		if err := json.Unmarshal(resp.Body, &reply); err != nil {
			return err
		}
		reply.Type = "txn_ok"

		return s.n.Reply(msg, reply)
	}
}

func (s *Server) Run() error {
	return s.n.Run()
}
