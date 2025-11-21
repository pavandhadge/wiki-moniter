package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/IBM/sarama"
)

type WikimediaEvent struct {
	Meta struct {
		Topic string `json:"topic"`
	} `json:"meta"`
	Wiki  string `json:"wiki"`
	User  string `json:"user"`
	Bot   bool   `json:"bot"`
	Type  string `json:"type"`
	Title string `json:"title"`
}

const wikiURL = "https://stream.wikimedia.org/v2/stream/recentchange"

func main() {
	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		brokers = "localhost:9092"
	}
	brokerList := strings.Split(brokers, ",")

	config := sarama.NewConfig()
	config.Producer.Return.Successes = true // required to get Successes()
	config.Producer.Return.Errors = true

	producer, _ := sarama.NewAsyncProducer(brokerList, config)

	// Handle errors
	go func() {
		for err := range producer.Errors() {
			log.Printf("FAILED: %v — %v", err.Msg.Topic, err.Err)
		}
	}()

	// Handle successes (optional)
	go func() {
		for msg := range producer.Successes() {
			log.Printf("ACK topic=%s partition=%d offset=%d", msg.Topic, msg.Partition, msg.Offset)
		}
	}()

	req, err := http.NewRequest("GET", wikiURL, nil)
	if err != nil {
		log.Fatalln(err)
	}
	req.Header.Set("User-Agent", "Wiki-moniter/1.0 (https://github.com/pavandhadge/wiki-moniter; professional0012345@gmail.com) Educational project")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Fatalln(err)
	}
	defer resp.Body.Close()

	reader := bufio.NewReader(resp.Body)
	fmt.Println("Streaming Wikimedia events to Kafka...")

	for {
		event, err := reader.ReadBytes('\n')
		if err != nil {
			log.Fatal("Stream error: ", err)
		}

		if len(event) < 6 || string(event[:6]) != "data: " {
			continue // skip keep-alive lines like ": \n" or empty
		}

		data := event[6:] // actual JSON payload

		var wikievent WikimediaEvent
		if err := json.Unmarshal(data, &wikievent); err != nil {
			log.Printf("JSON unmarshal error: %v | raw: %s", err, string(data))
			continue
		}

		// Filter out bot edits if you want (optional)
		// if wikievent.Bot { continue }

		msg := &sarama.ProducerMessage{
			Topic: "wikimedia.recentchange",
			Key:   sarama.StringEncoder(wikievent.Wiki + "|" + wikievent.Title),
			Value: sarama.ByteEncoder(data), // preserve original JSON exactly

			Headers: []sarama.RecordHeader{
				{Key: []byte("wiki"), Value: []byte(wikievent.Wiki)},
				{Key: []byte("event_type"), Value: []byte(wikievent.Type)},
				{Key: []byte("user"), Value: []byte(wikievent.User)},
				{Key: []byte("title"), Value: []byte(wikievent.Title)},
				{Key: []byte("is_bot"), Value: []byte(strconv.FormatBool(wikievent.Bot))},
			},

			Timestamp: time.Now(),
		}

		// Non-blocking send with backpressure handling
		select {
		case producer.Input() <- msg:
			// good
		default:
			log.Println("Producer backlog full - dropping event to avoid memory blowup")
		}
	}
}
