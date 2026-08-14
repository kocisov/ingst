package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	ingst "github.com/kocisov/ingst/sdk/go"
)

type orderCreated struct {
	OrderID int `json:"orderId"`
}

func main() {
	client, err := ingst.NewClient(
		environment("INGST_URL", "http://localhost:3000"),
		os.Getenv("INGST_API_KEY"),
	)
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	occurredAt := time.Now().UTC()

	event, err := client.CreateEvent(ctx, ingst.EventInput{
		Source:    "checkout",
		Type:      "order.created",
		Timestamp: occurredAt,
		Payload:   orderCreated{OrderID: 42},
	}, &ingst.CreateEventOptions{
		IdempotencyKey: fmt.Sprintf("order-42-created-%d", occurredAt.UnixNano()),
	})
	if err != nil {
		var apiError *ingst.APIError
		if errors.As(err, &apiError) {
			log.Fatalf("ingst returned %d (request %s): %s", apiError.StatusCode, apiError.RequestID, apiError)
		}
		log.Fatal(err)
	}

	payload, err := ingst.DecodePayload[orderCreated](event)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("created order %d at %s\n", payload.OrderID, event.Timestamp.Format(time.RFC3339))

	iterator := client.IterateEvents(ctx, ingst.ListEventsOptions{
		Source: "checkout",
		Type:   "order.created",
		Limit:  100,
	})
	for iterator.Next() {
		current := iterator.Event()
		fmt.Printf("%s %s\n", current.Timestamp.Format(time.RFC3339), current.Type)
	}
	if err := iterator.Err(); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatal(err)
	}
}

func environment(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
