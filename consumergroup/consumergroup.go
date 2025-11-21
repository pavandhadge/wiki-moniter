package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"time"
	shared "wikimedia-kafka-go/shared" // your shared types

	"github.com/IBM/sarama"
)

// Efficient file writer with buffering and proper locking
type eventWriter struct {
	file *os.File
	mu   sync.Mutex
}

var writer = &eventWriter{}

func init() {
	file, err := os.OpenFile("output.txt", os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		log.Fatalf("Failed to open output.txt: %v", err)
	}
	writer.file = file
}

func (w *eventWriter) WriteEvent(event shared.WikimediaEvent) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Use JSON for structured, readable, and parseable logs
	data, _ := json.Marshal(event)
	line := append(data, '\n')
	_, err := w.file.Write(line)
	return err
}

type WikiConsumer struct{}

func (WikiConsumer) Setup(sarama.ConsumerGroupSession) error {
	log.Println("Consumer group session started")
	return nil
}

func (WikiConsumer) Cleanup(sarama.ConsumerGroupSession) error {
	log.Println("Consumer group session ended")
	return nil
}

func (WikiConsumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		var event shared.WikimediaEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			log.Printf("JSON unmarshal error (offset %d): %v", msg.Offset, err)
			// Don't fail the whole claim on one bad message
			session.MarkMessage(msg, "")
			continue
		}

		// Optional: filter bots early
		if event.Bot {
			session.MarkMessage(msg, "")
			continue
		}

		// Log a clean summary
		log.Printf("[%s] %s edited %s | %s (offset=%d)",
			event.Wiki, event.User, event.Title, event.Type, msg.Offset)

		// Persist event efficiently
		if err := writer.WriteEvent(event); err != nil {
			log.Printf("Failed to write event to file: %v", err)
		}

		// Critical: commit offset
		session.MarkMessage(msg, "")
	}
	return nil
}

func main() {
	brokers := getEnv("KAFKA_BROKERS", "localhost:9092")
	topic := "wikimedia.recentchange"
	group := "wiki-monitor-group-v1"

	config := sarama.NewConfig()
	config.Version = sarama.V3_6_0_0 // Use recent Kafka version
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategySticky
	config.Consumer.Offsets.Initial = sarama.OffsetNewest // or OffsetOldest for backfill
	config.Consumer.Return.Errors = true
	config.Consumer.Group.Session.Timeout = 20 * time.Second
	config.Consumer.Group.Heartbeat.Interval = 3 * time.Second

	client, err := sarama.NewConsumerGroup(strings.Split(brokers, ","), group, config)
	if err != nil {
		log.Fatalf("Failed to create consumer group: %v", err)
	}
	defer client.Close()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			if err := client.Consume(ctx, []string{topic}, WikiConsumer{}); err != nil {
				if ctx.Err() != nil {
					return // shutdown signal
				}
				log.Printf("Consume error: %v", err)
			}
			if ctx.Err() != nil {
				return
			}
		}
	}()

	// Handle consumer errors in background
	go func() {
		for err := range client.Errors() {
			log.Printf("Consumer error: %v", err)
		}
	}()

	log.Printf("Consumer started | Group: %s | Topic: %s | Brokers: %s", group, topic, brokers)
	<-ctx.Done()
	log.Println("Shutting down gracefully...")

	wg.Wait()
	if err := writer.file.Close(); err != nil {
		log.Printf("Error closing output file: %v", err)
	}
	log.Println("Shutdown complete")
}

func getEnv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
