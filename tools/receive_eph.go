package tools

import (
	"context"
	"fmt"
	"log"

	"github.com/Azure/azure-amqp-common-go/conn"
	"github.com/Azure/azure-amqp-common-go/sas"
	eventhub "github.com/Azure/azure-event-hubs-go"
	"github.com/Azure/azure-event-hubs-go/eph"
	eventhubsstorage "github.com/Azure/azure-event-hubs-go/storage"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/Azure/go-autorest/autorest/azure"
)

// ReceiveViaEPH sets up an Event Processor Host (EPH), a small framework to
// receive events from several partitions.
func ReceiveViaEPH(ctx context.Context, nsName, hubName, storageAccountName, storageContainerName, storageAccountKey string) {

	// Azure Event Hub connection string
	eventHubConnStr := "xx"
	parsed, err := conn.ParsedConnectionFromStr(eventHubConnStr)

	// create an access token provider using AAD principal defined in environment
	tokenProvider, err := sas.NewTokenProvider(sas.TokenProviderWithKey(parsed.KeyName, parsed.Key))
	if err != nil {
		log.Fatalf("failed to configure AAD JWT provider: %s\n", err)
	}

	cred, err := azblob.NewSharedKeyCredential(storageAccountName, storageAccountKey)
	if err != nil {
		log.Fatalf("could not prepare a storage credential: %s\n", err)
	}

	// create a leaser and checkpointer backed by a storage container
	leaserCheckpointer, err := eventhubsstorage.NewStorageLeaserCheckpointer(
		cred,
		storageAccountName,
		storageContainerName,
		azure.PublicCloud)
	if err != nil {
		log.Fatalf("could not prepare a storage leaserCheckpointer: %s\n", err)
	}

	// use all of the above to create an Event Processor
	p, err := eph.New(
		ctx,
		nsName,
		hubName,
		tokenProvider,
		leaserCheckpointer,
		leaserCheckpointer,
		eph.WithNoBanner())
	if err != nil {
		log.Fatalf("failed to create EPH: %s\n", err)
	}

	// set up a handler for the Event Processor which will receive a single
	// message, print it to the console, and allow the process to exit
	// set up a channel to notify when event is successfully received
	eventReceived := make(chan struct{})

	handler := func(ctx context.Context, event *eventhub.Event) error {
		fmt.Printf("received: %s\n", string(event.Data))
		// notify channel that event was received
		//eventReceived <- struct{}{}
		return nil
	}

	// register the handler with the Event Processor
	// discard the HandlerID cause we don't need it
	_, err = p.RegisterHandler(ctx, handler)
	if err != nil {
		log.Fatalf("failed to set up handler: %s\n", err)
	}

	// finally, start the Event Processor with a timeout
	err = p.Start(ctx)
	if err != nil {
		log.Fatalf("failed to start EPH: %s\n", err)
	}

	// don't exit till event is received by handler
	select {
	case <-eventReceived:
	case err := <-ctx.Done():
		log.Fatalf("context cancelled before event received: %s\n", err)
	}

	p.Close(ctx)
}
