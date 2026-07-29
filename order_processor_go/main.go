package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"

	"github.com/joho/godotenv"
	amqp "github.com/rabbitmq/amqp091-go" // switched from streadway/amqp (unmaintained) to the maintained fork
)

// Struct to represent the message
type Message struct {
	ID       int    `json:"id"`
	Name     string `json:"name"`
	Quantity int    `json:"quantity"`
	IsOpen   bool   `json:"is_open"`
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

// sendPutRequest now returns an error instead of swallowing it,
// so the consumer loop can decide whether to ack or nack.
func sendPutRequest(url string, id int) error {
	fmt.Printf("Sending PUT request to URL: %s\n", url)
	data := map[string]bool{
		"is_open": true,
	}
	jsonStr, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("error marshaling payload: %w", err)
	}

	putURL := fmt.Sprintf("%s%d", url, id)

	req, err := http.NewRequest("PUT", putURL, bytes.NewBuffer(jsonStr))
	if err != nil {
		return fmt.Errorf("error creating request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("error sending request: %w", err)
	}
	defer resp.Body.Close()

	body, _ := ioutil.ReadAll(resp.Body)
	fmt.Println("Response:", string(body))

	// Treat non-2xx as a failure so it gets nack'd and retried/dead-lettered
	// instead of being silently ack'd.
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("PUT request failed with status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

func helloHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprint(w, "go server is running fine")
}

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/") // Replace with your RabbitMQ server URL
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	// Queue now declares a dead-letter exchange, so nack'd messages
	// (parse failures, failed PUT requests) route into the retry/DLQ
	// topology instead of vanishing or looping.
	q, err := ch.QueueDeclare(
		"queue1", // Queue name
		true,     // Durable
		false,    // Delete when unused
		false,    // Exclusive
		false,    // No-wait
		amqp.Table{
			"x-dead-letter-exchange":    "dlx.orders",
			"x-dead-letter-routing-key": "orders.process.dead",
		},
	)
	failOnError(err, "Failed to declare a queue")

	// Prefetch 1 so a single consumer processes one message at a time —
	// pairs well with explicit ack/nack below.
	err = ch.Qos(1, 0, false)
	failOnError(err, "Failed to set QoS")

	msgs, err := ch.Consume(
		q.Name, // Queue name
		"",     // Consumer name
		false,  // Auto-Ack -> now false, we ack/nack explicitly
		false,  // Exclusive
		false,  // No-local
		false,  // No-wait
		nil,    // Arguments
	)
	failOnError(err, "Failed to register a consumer")

	fmt.Printf("waiting for the new messages...")

	url := os.Getenv("ORDER_API_URL")

	http.HandleFunc("/go", helloHandler)
	serverAddr := os.Getenv("SERVER_ADDR")
	fmt.Printf("Server is running on %s\n", serverAddr)

	go func() {
		for d := range msgs {
			message := string(d.Body)
			fmt.Printf("Received a message: %s\n", message)

			var msg Message
			if err := json.Unmarshal(d.Body, &msg); err != nil {
				fmt.Println("Error parsing message:", err)
				// Malformed payload will never parse successfully on retry —
				// don't requeue to the same queue, send straight to DLX.
				if nackErr := d.Nack(false, false); nackErr != nil {
					fmt.Println("Error nacking message:", nackErr)
				}
				continue
			}

			if err := sendPutRequest(url, msg.ID); err != nil {
				fmt.Println("Error sending request:", err)
				// Could be transient (network blip, downstream 500) —
				// nack without requeue so it flows into the retry/DLQ
				// topology (delay queue with backoff, then DLQ after N attempts)
				// rather than looping instantly against this queue.
				if nackErr := d.Nack(false, false); nackErr != nil {
					fmt.Println("Error nacking message:", nackErr)
				}
				continue
			}

			if ackErr := d.Ack(false); ackErr != nil {
				fmt.Println("Error acking message:", ackErr)
			}
		}
	}()

	err = http.ListenAndServe(serverAddr, nil)
	if err != nil {
		fmt.Printf("Error starting server: %s\n", err)
	}

	select {}
}
