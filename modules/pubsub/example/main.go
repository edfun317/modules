package main

import (
	"context"
	"log"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/edfun317/modules/modules/pubsub"
)

func main() {
	// Create a new client
	client, err := pubsub.NewClient(
		pubsub.WithProjectID("your-project-id"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	// Set up subscriber
	messages, err := client.Subscribe(context.Background(), "example-topic")
	if err != nil {
		log.Fatal(err)
	}

	// Start consuming messages in a goroutine
	go func() {
		for msg := range messages {
			log.Printf("received message: %s", string(msg.Payload))
			msg.Ack()
		}
	}()

	// Publish a message
	msg := message.NewMessage(watermill.NewUUID(), []byte("Hello, GCP Pub/Sub!"))
	if err := client.Publish("example-topic", msg); err != nil {
		log.Fatal(err)
	}

	// Wait a bit to see the message
	time.Sleep(5 * time.Second)
}
