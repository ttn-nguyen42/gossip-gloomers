package server

import (
	"encoding/json"

	ms "github.com/jepsen-io/maelstrom/demo/go"
)

type TxOp struct {
	Op    string `json:"op"`
	Key   int    `json:"key"`
	Value int    `json:"value,omitempty"`
}

func NewTxOp(arr []any) *TxOp {
	var v int
	if arr[2] != nil {
		v = int(arr[2].(float64))
	}
	return &TxOp{
		Op:    arr[0].(string),
		Key:   int(arr[1].(float64)),
		Value: v,
	}
}

type Tx struct {
	Ops []*TxOp `json:"txn"`
}

func NewTx(msg ms.Message) *Tx {
	var tx Tx
	body := map[string]any{}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return nil
	}
	tx.Ops = make([]*TxOp, len(body["txn"].([]any)))
	for i, op := range body["txn"].([]any) {
		tx.Ops[i] = NewTxOp(op.([]any))
	}
	return &tx
}

type Reply struct {
	Type string  `json:"type"`
	Tx   []*TxOp `json:"txn,omitempty"`
}

func NewTxOkReply(tx *Tx) *Reply {
	return &Reply{
		Type: "txn_ok",
		Tx:   tx.Ops,
	}
}

func NewForwardTxnReply(tx *Tx) *Reply {
	return &Reply{
		Type: "forward_txn_ok",
		Tx:   tx.Ops,
	}
}

func NewInitOkReply() *Reply {
	return &Reply{
		Type: "init_ok",
	}
}
