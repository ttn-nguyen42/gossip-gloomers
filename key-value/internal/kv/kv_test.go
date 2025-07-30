package kv

import (
	"testing"
)

func TestNewStore(t *testing.T) {
	store := NewStore()
	if store == nil {
		t.Fatal("NewStore() returned nil")
	}
	if store.txId != 0 {
		t.Errorf("Expected txId to be 0, got %d", store.txId)
	}
	if len(store.txs) != 0 {
		t.Errorf("Expected empty transactions map, got %d", len(store.txs))
	}
}

func TestBegin(t *testing.T) {
	store := NewStore()

	tx1 := store.Begin()
	if tx1 == nil {
		t.Fatal("Begin() returned nil")
	}
	if tx1.id != 0 {
		t.Errorf("Expected tx id to be 0, got %d", tx1.id)
	}
	if tx1.store != store {
		t.Error("Transaction store reference is incorrect")
	}
	if tx1.commit || tx1.abort {
		t.Error("New transaction should not be committed or aborted")
	}

	tx2 := store.Begin()
	if tx2.id != 1 {
		t.Errorf("Expected tx id to be 1, got %d", tx2.id)
	}
}

func TestTransactionSetAndGet(t *testing.T) {
	store := NewStore()
	tx := store.Begin()

	err := tx.Set(1, 100)
	if err != nil {
		t.Errorf("Set failed: %v", err)
	}

	val, exists, err := tx.Get(1)
	if err != nil {
		t.Errorf("Get failed: %v", err)
	}
	if !exists {
		t.Error("Expected key to exist")
	}
	if val != 100 {
		t.Errorf("Expected value 100, got %d", val)
	}
}

func TestTransactionDelete(t *testing.T) {
	store := NewStore()
	tx := store.Begin()

	err := tx.Set(1, 100)
	if err != nil {
		t.Errorf("Set failed: %v", err)
	}

	err = tx.Delete(1)
	if err != nil {
		t.Errorf("Delete failed: %v", err)
	}

	_, exists, err := tx.Get(1)
	if err != nil {
		t.Errorf("Get failed: %v", err)
	}
	if exists {
		t.Error("Expected key to not exist after delete")
	}
}

func TestTransactionCommit(t *testing.T) {
	store := NewStore()
	tx := store.Begin()

	err := tx.Set(1, 100)
	if err != nil {
		t.Errorf("Set failed: %v", err)
	}

	err = tx.Commit()
	if err != nil {
		t.Errorf("Commit failed: %v", err)
	}

	if !tx.commit {
		t.Error("Transaction should be marked as committed")
	}

	if len(store.txs) != 0 {
		t.Error("Committed transaction should be removed from store")
	}
}

func TestTransactionAbort(t *testing.T) {
	store := NewStore()
	tx := store.Begin()

	err := tx.Set(1, 100)
	if err != nil {
		t.Errorf("Set failed: %v", err)
	}

	err = tx.Abort()
	if err != nil {
		t.Errorf("Abort failed: %v", err)
	}

	if !tx.abort {
		t.Error("Transaction should be marked as aborted")
	}

	if len(store.txs) != 0 {
		t.Error("Aborted transaction should be removed from store")
	}
}

func TestReadUncommitted(t *testing.T) {
	store := NewStore()

	tx1 := store.Begin()
	tx2 := store.Begin()

	err := tx1.Set(1, 100)
	if err != nil {
		t.Errorf("Set failed: %v", err)
	}

	val, exists, err := tx2.Get(1)
	if err != nil {
		t.Errorf("Get failed: %v", err)
	}
	if !exists {
		t.Error("Expected to see uncommitted value")
	}
	if val != 100 {
		t.Errorf("Expected value 100, got %d", val)
	}
}

func TestMultipleTransactions(t *testing.T) {
	store := NewStore()

	tx1 := store.Begin()
	tx2 := store.Begin()
	tx3 := store.Begin()

	err := tx1.Set(1, 100)
	if err != nil {
		t.Errorf("Set failed: %v", err)
	}

	err = tx2.Set(1, 200)
	if err != nil {
		t.Errorf("Set failed: %v", err)
	}

	val, exists, err := tx3.Get(1)
	if err != nil {
		t.Errorf("Get failed: %v", err)
	}
	if !exists {
		t.Error("Expected to see uncommitted value")
	}
	if val != 100 && val != 200 {
		t.Errorf("Expected to see one of the uncommitted values (100 or 200), got %d", val)
	}
}

func TestCommitAndRead(t *testing.T) {
	store := NewStore()

	tx1 := store.Begin()
	tx2 := store.Begin()

	err := tx1.Set(1, 100)
	if err != nil {
		t.Errorf("Set failed: %v", err)
	}

	err = tx1.Commit()
	if err != nil {
		t.Errorf("Commit failed: %v", err)
	}

	val, exists, err := tx2.Get(1)
	if err != nil {
		t.Errorf("Get failed: %v", err)
	}
	if !exists {
		t.Error("Expected to see committed value")
	}
	if val != 100 {
		t.Errorf("Expected value 100, got %d", val)
	}
}

func TestAbortAndRead(t *testing.T) {
	store := NewStore()

	tx1 := store.Begin()
	tx2 := store.Begin()

	err := tx1.Set(1, 100)
	if err != nil {
		t.Errorf("Set failed: %v", err)
	}

	err = tx1.Abort()
	if err != nil {
		t.Errorf("Abort failed: %v", err)
	}

	_, exists, err := tx2.Get(1)
	if err != nil {
		t.Errorf("Get failed: %v", err)
	}
	if exists {
		t.Error("Should not see aborted transaction's changes")
	}
}

func TestDoubleCommit(t *testing.T) {
	store := NewStore()
	tx := store.Begin()

	err := tx.Commit()
	if err != nil {
		t.Errorf("First commit failed: %v", err)
	}

	err = tx.Commit()
	if err == nil {
		t.Error("Expected error on double commit")
	}
}

func TestDoubleAbort(t *testing.T) {
	store := NewStore()
	tx := store.Begin()

	err := tx.Abort()
	if err != nil {
		t.Errorf("First abort failed: %v", err)
	}

	err = tx.Abort()
	if err == nil {
		t.Error("Expected error on double abort")
	}
}

func TestCommitAfterAbort(t *testing.T) {
	store := NewStore()
	tx := store.Begin()

	err := tx.Abort()
	if err != nil {
		t.Errorf("Abort failed: %v", err)
	}

	err = tx.Commit()
	if err == nil {
		t.Error("Expected error on commit after abort")
	}
}

func TestAbortAfterCommit(t *testing.T) {
	store := NewStore()
	tx := store.Begin()

	err := tx.Commit()
	if err != nil {
		t.Errorf("Commit failed: %v", err)
	}

	err = tx.Abort()
	if err == nil {
		t.Error("Expected error on abort after commit")
	}
}

func TestSetAfterCommit(t *testing.T) {
	store := NewStore()
	tx := store.Begin()

	err := tx.Commit()
	if err != nil {
		t.Errorf("Commit failed: %v", err)
	}

	err = tx.Set(1, 100)
	if err == nil {
		t.Error("Expected error on set after commit")
	}
}

func TestGetAfterCommit(t *testing.T) {
	store := NewStore()
	tx := store.Begin()

	err := tx.Commit()
	if err != nil {
		t.Errorf("Commit failed: %v", err)
	}

	_, _, err = tx.Get(1)
	if err == nil {
		t.Error("Expected error on get after commit")
	}
}

func TestDeleteAfterCommit(t *testing.T) {
	store := NewStore()
	tx := store.Begin()

	err := tx.Commit()
	if err != nil {
		t.Errorf("Commit failed: %v", err)
	}

	err = tx.Delete(1)
	if err == nil {
		t.Error("Expected error on delete after commit")
	}
}

func TestSetAfterAbort(t *testing.T) {
	store := NewStore()
	tx := store.Begin()

	err := tx.Abort()
	if err != nil {
		t.Errorf("Abort failed: %v", err)
	}

	err = tx.Set(1, 100)
	if err == nil {
		t.Error("Expected error on set after abort")
	}
}

func TestGetAfterAbort(t *testing.T) {
	store := NewStore()
	tx := store.Begin()

	err := tx.Abort()
	if err != nil {
		t.Errorf("Abort failed: %v", err)
	}

	_, _, err = tx.Get(1)
	if err == nil {
		t.Error("Expected error on get after abort")
	}
}

func TestDeleteAfterAbort(t *testing.T) {
	store := NewStore()
	tx := store.Begin()

	err := tx.Abort()
	if err != nil {
		t.Errorf("Abort failed: %v", err)
	}

	err = tx.Delete(1)
	if err == nil {
		t.Error("Expected error on delete after abort")
	}
}

func TestConcurrentTransactions(t *testing.T) {
	store := NewStore()

	tx1 := store.Begin()
	tx2 := store.Begin()

	err := tx1.Set(1, 100)
	if err != nil {
		t.Errorf("Set failed: %v", err)
	}

	err = tx2.Set(2, 200)
	if err != nil {
		t.Errorf("Set failed: %v", err)
	}

	val1, exists1, err := tx2.Get(1)
	if err != nil {
		t.Errorf("Get failed: %v", err)
	}
	if !exists1 {
		t.Error("Expected to see uncommitted value")
	}
	if val1 != 100 {
		t.Errorf("Expected value 100, got %d", val1)
	}

	val2, exists2, err := tx1.Get(2)
	if err != nil {
		t.Errorf("Get failed: %v", err)
	}
	if !exists2 {
		t.Error("Expected to see uncommitted value")
	}
	if val2 != 200 {
		t.Errorf("Expected value 200, got %d", val2)
	}
}

func TestDeleteAndSet(t *testing.T) {
	store := NewStore()
	tx := store.Begin()

	err := tx.Set(1, 100)
	if err != nil {
		t.Errorf("Set failed: %v", err)
	}

	err = tx.Delete(1)
	if err != nil {
		t.Errorf("Delete failed: %v", err)
	}

	err = tx.Set(1, 200)
	if err != nil {
		t.Errorf("Set failed: %v", err)
	}

	val, exists, err := tx.Get(1)
	if err != nil {
		t.Errorf("Get failed: %v", err)
	}
	if !exists {
		t.Error("Expected key to exist after set")
	}
	if val != 200 {
		t.Errorf("Expected value 200, got %d", val)
	}
}

func TestSetAndDelete(t *testing.T) {
	store := NewStore()
	tx := store.Begin()

	err := tx.Set(1, 100)
	if err != nil {
		t.Errorf("Set failed: %v", err)
	}

	err = tx.Delete(1)
	if err != nil {
		t.Errorf("Delete failed: %v", err)
	}

	_, exists, err := tx.Get(1)
	if err != nil {
		t.Errorf("Get failed: %v", err)
	}
	if exists {
		t.Error("Expected key to not exist after delete")
	}
}

func TestNonExistentKey(t *testing.T) {
	store := NewStore()
	tx := store.Begin()

	_, exists, err := tx.Get(999)
	if err != nil {
		t.Errorf("Get failed: %v", err)
	}
	if exists {
		t.Error("Expected key to not exist")
	}
}
