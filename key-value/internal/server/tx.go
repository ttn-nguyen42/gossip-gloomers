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
	return &TxOp{
		Op:    arr[0].(string),
		Key:   int(arr[1].(float64)),
		Value: int(arr[2].(float64)),
	}
}

type Tx struct {
	Ops []*TxOp
}

func NewTx(msg ms.Message) *Tx {
	var tx Tx
	body := map[string]any{}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return nil
	}
	tx.Ops = make([]*TxOp, len(body["tx"].([]any)))
	for i, op := range body["tx"].([]any) {
		tx.Ops[i] = NewTxOp(op.([]any))
	}
	return &tx
}

type Reply struct {
	Type string  `json:"type"`
	Tx   []*TxOp `json:"tx,omitempty"`
}

func NewTxOkReply(tx *Tx) *Reply {
	return &Reply{
		Type: "tx_ok",
		Tx:   tx.Ops,
	}
}

func NewInitOkReply() *Reply {
	return &Reply{
		Type: "init_ok",
	}
}
