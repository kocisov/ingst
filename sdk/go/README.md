# ingst Go SDK

> Go client for the ingst event ingestion API.

```go
package main

import (
	"context"
	"log"
	"os"
	"time"

	ingst "github.com/kocisov/ingst/sdk/go"
)

func main() {
	client, err := ingst.NewClient("https://events.example.com", os.Getenv("INGST_API_KEY"))
	if err != nil {
		log.Fatal(err)
	}

	_, err = client.CreateEvent(context.Background(), ingst.EventInput{
		Source:    "checkout",
		Type:      "order.created",
		Timestamp: time.Now(),
		Payload:   map[string]any{"orderId": 42},
	}, &ingst.CreateEventOptions{IdempotencyKey: "order-42-created"})
	if err != nil {
		log.Fatal(err)
	}
}
```

Use `IterateEvents` for automatic cursor pagination and `DecodePayload[T]` to
decode an event payload into an application type.
