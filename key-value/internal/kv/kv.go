package kv

import (
	"fmt"
	"sync"
)

type Store struct {
	mu   sync.RWMutex
	mem  sync.Map
	txs  map[int]*Tx
	txId int
}

type Tx struct {
	id      int
	store   *Store
	commit  bool
	abort   bool
	upds    map[int]int
	deletes map[int]struct{}
}

func NewStore() *Store {
	return &Store{
		mem:  sync.Map{},
		txs:  make(map[int]*Tx),
		txId: 0,
	}
}

func (s *Store) Begin() *Tx {
	s.mu.Lock()
	defer s.mu.Unlock()
	txId := s.txId
	s.txId += 1
	tx := &Tx{
		id:      txId,
		store:   s,
		upds:    make(map[int]int),
		deletes: make(map[int]struct{}),
	}
	s.txs[txId] = tx
	return tx
}

func (tx *Tx) Commit() error {
	if err := tx.ensure(); err != nil {
		return err
	}
	tx.store.mu.Lock()
	defer tx.store.mu.Unlock()

	for key, val := range tx.upds {
		tx.store.mem.Store(key, val)
	}
	for key := range tx.deletes {
		tx.store.mem.Delete(key)
	}
	tx.commit = true
	delete(tx.store.txs, tx.id)
	return nil
}

func (tx *Tx) Abort() error {
	if err := tx.ensure(); err != nil {
		return err
	}
	tx.store.mu.Lock()
	defer tx.store.mu.Unlock()

	tx.abort = true
	delete(tx.store.txs, tx.id)
	return nil
}

func (tx *Tx) Get(key int) (int, bool, error) {
	if err := tx.ensure(); err != nil {
		return 0, false, err
	}
	val, ok := tx.get(key)
	if ok {
		return val, true, nil
	}
	tx.store.mu.RLock()
	defer tx.store.mu.RUnlock()

	for id, otherTx := range tx.store.txs {
		if err := otherTx.ensure(); id != tx.id && err == nil {
			val, ok := otherTx.get(key)
			if ok {
				return val, true, nil
			}
		}
	}

	anyval, ok := tx.store.mem.Load(key)
	if ok {
		return anyval.(int), true, nil
	}
	return 0, false, nil
}

func (tx *Tx) get(key int) (int, bool) {
	val, ok := tx.upds[key]
	if ok {
		return val, true
	}
	_, ok = tx.deletes[key]
	if ok {
		return 0, false
	}
	return 0, false
}

func (tx *Tx) Set(key int, val int) error {
	if err := tx.ensure(); err != nil {
		return err
	}
	tx.upds[key] = val
	delete(tx.deletes, key)
	return nil
}

func (tx *Tx) Delete(key int) error {
	if err := tx.ensure(); err != nil {
		return err
	}
	tx.deletes[key] = struct{}{}
	delete(tx.upds, key)
	return nil
}

func (tx *Tx) ensure() error {
	if tx.commit {
		return fmt.Errorf("tx already committed")
	}
	if tx.abort {
		return fmt.Errorf("tx already aborted")
	}
	return nil
}
