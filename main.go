package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"strconv"
	"time"

	eventhub "github.com/Azure/azure-event-hubs-go"
	"github.com/google/uuid"
)

type Payload struct {
	Uid  string	      `json:"uid"`
	Data string       `json:"data"`
}

// keeps channel per request
var rchans = make(map[string](chan string))

func handleReq(p Payload, ctx context.Context, hub *eventhub.Hub) {
	// parse z and get uid
	uid := p.Uid
  
	// create channel and add to rchans with uid
	c := make(chan string, 5)
	rchans[uid] = c
	
	fmt.Println(&rchans)

	// List all Entities on the EventHub we will be listening to
	topics := []string{"topic1"}
	for _, topic := range topics {
		fmt.Println(topic)
		fmt.Println(uid)
		
		// Marshal the input string
		bs, _ := json.Marshal(p)

		//TODO Need to know how to add Properties in the Event
		event := eventhub.NewEventFromString(string(bs))
		sendEvent(ctx, hub, event)
	}

	// wait for response 
	waitResp(uid, c, len(topics))
}

func waitResp(uid string, c chan string, noPeers int) {
	// keep recived response count
  	var i int = 0
  
  	// slice to aggregate responses
	responses := []string{}
  
	for {
		select {
		case r := <-c:
			
			fmt.Println(responses)
			// append response
			responses = append(responses, r)

			i = i + 1
			if i == noPeers {
				// all peer responses received
				// TODO do whatever the operation

				// remove channel
				delete(rchans, uid)
			}
		}
	}
}

func main() {
	connStr := "xx"
	hub, err := eventhub.NewHubFromConnectionString(connStr)

	if err != nil {
		fmt.Println(err)
		return
	}
	
	// Connect and timeout after 20 seconds
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// Just Send It
	for i := 0; i < 5; i++ {
		// Send 5 messages with Property		
		var p Payload // dot notation
		p.Uid = uuid.NewString()
		p.Data = String(10) + " " + strconv.Itoa(i)
		go handleReq(p, ctx, hub)
		
		
		// // Marshal the input string
		// bs, _ := json.Marshal(p)

		// //TODO Need to know how to add Properties in the Event
		// event := eventhub.NewEventFromString(string(bs))
		// sendEvent(ctx, hub, event)
	}
	// listen to each partition of the Event Hub
	runtimeInfo, err := hub.GetRuntimeInformation(ctx)
	if err != nil {
		fmt.Println(err)
		return
	}
	
	fmt.Println(runtimeInfo.PartitionIDs)
		
	// Receive Events from the topic
	recieveEvent(ctx, hub, runtimeInfo.PartitionIDs)
	
}

func sendEvent(c context.Context, h *eventhub.Hub, e *eventhub.Event) {
	println("Sending ", string(e.Data))
	err := h.Send(c, e)
	if err != nil {
		fmt.Println(err)
		return
	}
}

// How to return a value in go?
func recieveEvent(c context.Context, h *eventhub.Hub, partitionIDs []string) {
	handler := func(c context.Context, event *eventhub.Event) error {
		fmt.Println(string(event.Data))
				
		var z Payload
		bs := []byte(event.Data)
		json.Unmarshal(bs, &z)
		fmt.Println(z)
		
		fmt.Println("UUID:", z.Uid)

		// parse z and get uid
		uid := z.Uid
		
		fmt.Println(&rchans)
		
		// find matching channel with uid
		// send z to that channel
		if c, ok := rchans[uid]; ok {
			fmt.Println("Found matching uid ", uid)
			c <- string(bs)
		}
		
		return nil
	}
	
	for _, partitionID := range partitionIDs { 
		// Start receiving messages 
		_, err := h.Receive(c, partitionID, handler, eventhub.ReceiveWithLatestOffset())
		if err != nil {
			fmt.Println(err)
			return
		}
		
    }
}


// Some Random generators

const charset = "abcdefghijklmnopqrstuvwxyz" +
  "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

var seededRand *rand.Rand = rand.New(
  rand.NewSource(time.Now().UnixNano()))

func StringWithCharset(length int, charset string) string {
  b := make([]byte, length)
  for i := range b {
    b[i] = charset[seededRand.Intn(len(charset))]
  }
  return string(b)
}

func String(length int) string {
  return StringWithCharset(length, charset)
}