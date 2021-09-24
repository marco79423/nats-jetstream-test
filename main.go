package main

import (
	"log"

	"github.com/marco79423/nats-jetstream-test/tester"
)


func main() {
	if err := tester.RunTesters(); err != nil {
		log.Fatalf("%+v", err)
	}
}
