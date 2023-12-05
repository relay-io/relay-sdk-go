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

var _ client.Runner[payload, state] = (*Processor)(nil)

type Processor struct {
}

func (p *Processor) Run(ctx context.Context, helper client.JobHelper[payload, state]) {
	// to something with job & state
	job := helper.Job()
	state := job.State.UnwrapOrDefault()
	fmt.Println(job, state)

	err := helper.Complete(ctx)
	if err != nil {
		panic(err)
	}
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

	poller := relayClient.Poller(queue, new(Processor)).NumWorkers(5).Build()
	if err != nil {
		panic(err)
	}

	err = poller.Start(ctx)
	if err != nil {
		panic(err)
	}
}
