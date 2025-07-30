package main

import (
	"log"

	ms "github.com/jepsen-io/maelstrom/demo/go"
	"github.com/ttn-nguyen42/key-value/internal/server"
)

func main() {
	n := ms.NewNode()
	s := server.NewServer(n)

	if err := s.Run(); err != nil {
		log.Fatal(err)
	}
}
