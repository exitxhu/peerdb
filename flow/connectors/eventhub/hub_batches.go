package conneventhub

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	azeventhubs "github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

// multimap from ScopedEventhub to *azeventhubs.EventDataBatch
type HubBatches struct {
	batches map[ScopedEventhub]*azeventhubs.EventDataBatch
	manager *EventHubManager
}

func NewHubBatches(manager *EventHubManager) *HubBatches {
	return &HubBatches{
		batches: make(map[ScopedEventhub]*azeventhubs.EventDataBatch),
		manager: manager,
	}
}

func (h *HubBatches) AddEvent(ctx context.Context, name ScopedEventhub, event string) error {
	batch, ok := h.batches[name]
	if !ok {
		newBatch, err := h.manager.CreateEventDataBatch(ctx, name)
		if err != nil {
			return err
		}
		batch = newBatch
	}

	err := tryAddEventToBatch(event, batch)
	if err != nil {
		if strings.Contains(err.Error(), "too large for the batch") {
			log.WithFields(log.Fields{
				"event": event,
			}).Infof("event too large for batch, sending batch - %s", name)
			err = h.sendBatch(ctx, name, batch)
			if err != nil {
				return fmt.Errorf("failed to send batch: %v", err)
			}
			delete(h.batches, name)
		} else {
			return fmt.Errorf("failed to add event data: %v", err)
		}
	} else {
		h.batches[name] = batch
	}

	return nil
}

func (h *HubBatches) Len() int {
	return len(h.batches)
}

// ForEach calls the given function for each ScopedEventhub and batch pair
func (h *HubBatches) ForEach(fn func(ScopedEventhub, *azeventhubs.EventDataBatch)) {
	for name, batches := range h.batches {
		fn(name, batches)
	}
}

func (h *HubBatches) sendBatch(
	ctx context.Context,
	tblName ScopedEventhub,
	events *azeventhubs.EventDataBatch,
) error {
	subCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	hub, err := h.manager.GetOrCreateHubClient(subCtx, tblName)
	if err != nil {
		return err
	}

	opts := &azeventhubs.SendEventDataBatchOptions{}
	err = hub.SendEventDataBatch(subCtx, events, opts)
	if err != nil {
		return err
	}

	log.Infof("successfully sent %d events to event hub topic - %s", events.NumEvents(), tblName.ToString())
	return nil
}

func (h *HubBatches) flushAllBatches(
	ctx context.Context,
	maxParallelism int64,
	flowName string,
) error {
	if h.Len() == 0 {
		log.WithFields(log.Fields{
			"flowName": flowName,
		}).Infof("no events to send")
		return nil
	}

	var numEventsPushed int32
	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(int(maxParallelism))
	h.ForEach(func(tblName ScopedEventhub, eventBatch *azeventhubs.EventDataBatch) {
		g.Go(func() error {
			numEvents := eventBatch.NumEvents()
			err := h.sendBatch(gCtx, tblName, eventBatch)
			if err != nil {
				return err
			}

			atomic.AddInt32(&numEventsPushed, numEvents)
			log.WithFields(log.Fields{
				"flowName": flowName,
			}).Infof("pushed %d events to event hub: %s", numEvents, tblName)
			return nil
		})
	})

	log.Infof("[sendEventBatch] successfully sent %d events in total to event hub",
		numEventsPushed)
	return g.Wait()
}

// Clear removes all batches from the HubBatches
func (h *HubBatches) Clear() {
	h.batches = make(map[ScopedEventhub]*azeventhubs.EventDataBatch)
}

func tryAddEventToBatch(event string, batch *azeventhubs.EventDataBatch) error {
	eventData := eventDataFromString(event)
	opts := &azeventhubs.AddEventDataOptions{}
	return batch.AddEventData(eventData, opts)
}

func eventDataFromString(s string) *azeventhubs.EventData {
	return &azeventhubs.EventData{
		Body: []byte(s),
	}
}
