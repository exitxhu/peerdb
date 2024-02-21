package conneventhub

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/eventhub/armeventhub"
	cmap "github.com/orcaman/concurrent-map/v2"

	"github.com/PeerDB-io/peer-flow/connectors/utils"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/logger"
)

type EventHubManager struct {
	creds *azidentity.DefaultAzureCredential
	// eventhub peer name -> config
	peerConfig cmap.ConcurrentMap[string, *protos.EventHubConfig]
	// eventhub name -> client
	hubs sync.Map
	// eventhub name -> number of partitions
	partitionCount sync.Map
}

func NewEventHubManager(
	creds *azidentity.DefaultAzureCredential,
	groupConfig *protos.EventHubGroupConfig,
) *EventHubManager {
	peerConfig := cmap.New[*protos.EventHubConfig]()

	for name, config := range groupConfig.Eventhubs {
		peerConfig.Set(name, config)
	}

	return &EventHubManager{
		creds:      creds,
		peerConfig: peerConfig,
	}
}

func (m *EventHubManager) GetNumPartitions(ctx context.Context, name ScopedEventhub) (int, error) {
	partitionCount, ok := m.partitionCount.Load(name)
	if ok {
		return partitionCount.(int), nil
	}

	hub, err := m.GetOrCreateHubClient(ctx, name)
	if err != nil {
		return 0, err
	}

	props, err := hub.GetEventHubProperties(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to get eventhub properties: %v", err)
	}

	numPartitions := len(props.PartitionIDs)
	m.partitionCount.Store(name, numPartitions)

	return numPartitions, nil
}

func (m *EventHubManager) GetOrCreateHubClient(ctx context.Context, name ScopedEventhub) (
	*azeventhubs.ProducerClient, error,
) {
	ehConfig, ok := m.peerConfig.Get(name.PeerName)
	if !ok {
		return nil, fmt.Errorf("eventhub '%s' not configured", name.Eventhub)
	}

	namespace := ehConfig.Namespace
	// if the namespace isn't fully qualified, add the `.servicebus.windows.net`
	// check by counting the number of '.' in the namespace
	if strings.Count(namespace, ".") < 2 {
		namespace = fmt.Sprintf("%s.servicebus.windows.net", namespace)
	}

	var hubConnectOK bool
	var hub any
	hub, hubConnectOK = m.hubs.Load(name.ToString())
	if hubConnectOK {
		hubTmp := hub.(*azeventhubs.ProducerClient)
		_, err := hubTmp.GetEventHubProperties(ctx, nil)
		if err != nil {
			logger := logger.LoggerFromCtx(ctx)
			logger.Info(
				fmt.Sprintf("eventhub %s not reachable. Will re-establish connection and re-create it.", name),
				slog.Any("error", err))
			closeError := m.closeProducerClient(ctx, hubTmp)
			if closeError != nil {
				logger.Error("failed to close producer client", slog.Any("error", closeError))
			}
			m.hubs.Delete(name.ToString())
			hubConnectOK = false
		}
	}

	if !hubConnectOK {
		opts := &azeventhubs.ProducerClientOptions{
			RetryOptions: azeventhubs.RetryOptions{
				MaxRetries:    32,
				RetryDelay:    2 * time.Second,
				MaxRetryDelay: 16 * time.Second,
			},
		}
		hub, err := azeventhubs.NewProducerClient(namespace, name.Eventhub, m.creds, opts)
		if err != nil {
			return nil, fmt.Errorf("failed to create eventhub client: %v", err)
		}
		m.hubs.Store(name.ToString(), hub)
		return hub, nil
	}

	return hub.(*azeventhubs.ProducerClient), nil
}

func (m *EventHubManager) closeProducerClient(ctx context.Context, pc *azeventhubs.ProducerClient) error {
	if pc != nil {
		return pc.Close(ctx)
	}
	return nil
}

func (m *EventHubManager) Close(ctx context.Context) error {
	numHubsClosed := atomic.Uint32{}
	shutdown := utils.HeartbeatRoutine(ctx, func() string {
		return fmt.Sprintf("closed %d eventhub clients", numHubsClosed.Load())
	})
	defer shutdown()

	var allErrors error
	numHubsClosed := atomic.Uint32{}
	shutdown := utils.HeartbeatRoutine(ctx, func() string {
		return fmt.Sprintf("closed %d eventhub clients", numHubsClosed.Load())
	})
	defer shutdown()
	m.hubs.Range(func(key any, value any) bool {
		slog.InfoContext(ctx, "closing eventhub client",
			slog.Uint64("numClosed", uint64(numHubsClosed.Load())),
			slog.String("Currently closing", fmt.Sprintf("%v", key)))
		client := value.(*azeventhubs.ProducerClient)
		err := m.closeProducerClient(ctx, client)
		if err != nil {
			logger.LoggerFromCtx(ctx).Error(fmt.Sprintf("failed to close eventhub client for %v", name), slog.Any("error", err))
			allErrors = errors.Join(allErrors, err)
		}
		numHubsClosed.Add(1)
		return true
	})

	slog.InfoContext(ctx, "closed all eventhub clients", slog.Any("numClosed", numHubsClosed.Load()))

	return allErrors
}

func (m *EventHubManager) CreateEventDataBatch(ctx context.Context, destination ScopedEventhub) (
	*azeventhubs.EventDataBatch, error,
) {
	hub, err := m.GetOrCreateHubClient(ctx, destination)
	if err != nil {
		return nil, err
	}

	opts := &azeventhubs.EventDataBatchOptions{
		// We want to route same hashed partition value
		// to same partition.
		PartitionID: &destination.PartitionKeyValue,
	}
	batch, err := hub.NewEventDataBatch(ctx, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to create event data batch: %v", err)
	}

	return batch, nil
}

// EnsureEventHubExists ensures that the eventhub exists.
func (m *EventHubManager) EnsureEventHubExists(ctx context.Context, name ScopedEventhub) error {
	cfg, ok := m.peerConfig.Get(name.PeerName)
	if !ok {
		return fmt.Errorf("eventhub peer '%s' not configured", name.PeerName)
	}

	hubClient, err := m.getEventHubMgmtClient(cfg.SubscriptionId)
	if err != nil {
		return fmt.Errorf("failed to get event hub client: %v", err)
	}

	namespace := cfg.Namespace
	resourceGroup := cfg.ResourceGroup

	_, err = hubClient.Get(ctx, resourceGroup, namespace, name.Eventhub, nil)

	partitionCount := int64(cfg.PartitionCount)
	retention := int64(cfg.MessageRetentionInDays)
	logger := logger.LoggerFromCtx(ctx)
	if err != nil {
		opts := armeventhub.Eventhub{
			Properties: &armeventhub.Properties{
				PartitionCount:         &partitionCount,
				MessageRetentionInDays: &retention,
			},
		}

		_, err := hubClient.CreateOrUpdate(ctx, resourceGroup, namespace, name.Eventhub, opts, nil)
		if err != nil {
			slog.Error("failed to create event hub", slog.Any("error", err))
			return err
		}

		logger.Info("event hub created", slog.Any("name", name))
	} else {
		logger.Info("event hub exists already", slog.Any("name", name))
	}

	return nil
}

func (m *EventHubManager) getEventHubMgmtClient(subID string) (*armeventhub.EventHubsClient, error) {
	if subID == "" {
		envSubID, err := utils.GetAzureSubscriptionID()
		if err != nil {
			slog.Error("failed to get azure subscription id", slog.Any("error", err))
			return nil, err
		}
		subID = envSubID
	}

	hubClient, err := armeventhub.NewEventHubsClient(subID, m.creds, nil)
	if err != nil {
		slog.Error("failed to get event hub client", slog.Any("error", err))
		return nil, err
	}

	return hubClient, nil
}
