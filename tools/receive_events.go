package tools

import (
	"context"
	"fmt"
	"log"

	"github.com/Azure/azure-amqp-common-go/aad"
	"github.com/Azure/azure-amqp-common-go/persist"
	eventhub "github.com/Azure/azure-event-hubs-go"
)

func Receive(ctx context.Context, hub *eventhub.Hub, partitionIDs []string) {
	// set up wait group to wait for expected message
	eventReceived := make(chan struct{})

	// declare handler for incoming events
	handler := func(ctx context.Context, event *eventhub.Event) error {
		fmt.Printf("received: %s\n", string(event.Data))
		// notify channel that event was received
		eventReceived <- struct{}{}
		return nil
	}

	fmt.Println("Offset is", persist.StartOfStream)
	for _, partitionID := range partitionIDs {
		_, err := hub.Receive(
			ctx,
			partitionID,
			handler,
			eventhub.ReceiveWithConsumerGroup("$Default"),
			//eventhubs.ReceiveWithStartingOffset(persist.StartOfStream),
		)
		if err != nil {
			log.Fatalf("failed to receive for partition ID %s: %s\n", partitionID, err)
		}
	}

	// don't exit till event is received by handler
	select {
	case <-eventReceived:
	case err := <-ctx.Done():
		log.Fatalf("context cancelled before event received: %s\n", err)
	}
}

func ReceiveUsingTokenProvider(ctx context.Context, nsName, hubName string) {
	// create an access token provider using AAD principal defined in environment
	provider, err := aad.NewJWTProvider(aad.JWTProviderWithEnvironmentVars())
	if err != nil {
		log.Fatalf("failed to configure AAD JWT provider: %s\n", err)
	}

	// get an existing hub for dataplane use
	hub, err := eventhub.NewHub(nsName, hubName, provider)
	if err != nil {
		log.Fatalf("failed to get hub: %s\n", err)
	}

	// get info about the hub, particularly number and IDs of partitions
	info, err := hub.GetRuntimeInformation(ctx)
	if err != nil {
		log.Fatalf("failed to get runtime info: %s\n", err)
	}
	log.Printf("partition IDs: %s\n", info.PartitionIDs)

	// set up wait group to wait for expected message
	eventReceived := make(chan struct{})

	// declare handler for incoming events
	handler := func(ctx context.Context, event *eventhub.Event) error {
		fmt.Printf("received: %s\n", string(event.Data))
		// notify channel that event was received
		eventReceived <- struct{}{}
		return nil
	}

	fmt.Println("Offset", persist.StartOfStream)

	for _, partitionID := range info.PartitionIDs {
		_, err := hub.Receive(
			ctx,
			partitionID,
			handler,
			//eventhubs.ReceiveWithLatestOffset(),
			eventhub.ReceiveWithStartingOffset(persist.StartOfStream),
		)
		if err != nil {
			log.Fatalf("failed to receive for partition ID %s: %s\n", partitionID, err)
		}
	}

	// don't exit till event is received by handler
	select {
	case <-eventReceived:
	case err := <-ctx.Done():
		log.Fatalf("context cancelled before event received: %s\n", err)
	}
}
