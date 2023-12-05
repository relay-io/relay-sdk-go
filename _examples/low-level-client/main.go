package main

import (
	"context"
	"fmt"
	. "github.com/go-playground/pkg/v5/values/option"
	"github.com/relay-io/relay-sdk-go/core/job"
	"github.com/relay-io/relay-sdk-go/http/client"
)

type payload struct {
	CustomerID string `json:"customer_id"`
}

type state struct {
	Processed int `json:"processed"`
	Offset    int `json:"offset"`
}

func main() {
	ctx := context.Background()

	relayClient := client.NewBuilder[payload, state]("http://localhost:8080").Build()
	queue := "my-queue"
	err := relayClient.Enqueue(ctx, job.Unique, []job.New[payload, state]{
		{
			Queue:      queue,
			ID:         "unique-queues-job-id",
			Timeout:    30,
			MaxRetries: None[int16](),
			Payload: payload{
				CustomerID: "unique-customer-account-id",
			},
			State: Some(state{
				Processed: 0,
				Offset:    0,
			}),
		},
	})
	if err != nil {
		panic(err)
	}

	jobs, err := relayClient.Poll(ctx, queue, 1)
	if err != nil {
		panic(err)
	}

	for _, j := range jobs {
		// do something with job
		fmt.Println(j)

		err = relayClient.Complete(ctx, queue, j.ID, j.RunID.Unwrap())
		if err != nil {
			panic(err)
		}
	}
}
