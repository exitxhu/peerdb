package main

import (
	"context"
	"fmt"

	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/shared"
	peerflow "github.com/PeerDB-io/peer-flow/workflows"
	"github.com/google/uuid"
	"go.temporal.io/sdk/client"
)

// grpc server implementation
type FlowRequestHandler struct {
	temporalClient client.Client
	protos.UnimplementedFlowServiceServer
}

func NewFlowRequestHandler(temporalClient client.Client) *FlowRequestHandler {
	return &FlowRequestHandler{
		temporalClient: temporalClient,
	}
}

func (h *FlowRequestHandler) CreatePeerFlow(
	ctx context.Context, req *protos.CreatePeerFlowRequest) (*protos.CreatePeerFlowResponse, error) {
	cfg := req.ConnectionConfigs
	workflowID := fmt.Sprintf("%s-peerflow-%s", cfg.FlowJobName, uuid.New())
	workflowOptions := client.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: shared.PeerFlowTaskQueue,
	}

	maxBatchSize := int(cfg.MaxBatchSize)
	if maxBatchSize == 0 {
		maxBatchSize = 100000
		cfg.MaxBatchSize = uint32(maxBatchSize)
	}

	limits := &peerflow.PeerFlowLimits{
		TotalSyncFlows:      0,
		TotalNormalizeFlows: 0,
		MaxBatchSize:        maxBatchSize,
	}

	state := peerflow.NewStartedPeerFlowState()
	_, err := h.temporalClient.ExecuteWorkflow(
		ctx,                                 // context
		workflowOptions,                     // workflow start options
		peerflow.PeerFlowWorkflowWithConfig, // workflow function
		cfg,                                 // workflow input
		limits,                              // workflow limits
		state,                               // workflow state
	)
	if err != nil {
		return nil, fmt.Errorf("unable to start PeerFlow workflow: %w", err)
	}

	return &protos.CreatePeerFlowResponse{
		WorflowId: workflowID,
	}, nil
}

func (h *FlowRequestHandler) CreateQRepFlow(
	ctx context.Context, req *protos.CreateQRepFlowRequest) (*protos.CreateQRepFlowResponse, error) {
	lastPartition := &protos.QRepPartition{
		PartitionId: "not-applicable-partition",
		Range:       nil,
	}

	cfg := req.QrepConfig
	workflowID := fmt.Sprintf("%s-qrepflow-%s", cfg.FlowJobName, uuid.New())
	workflowOptions := client.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: shared.PeerFlowTaskQueue,
	}

	numPartitionsProcessed := 0
	_, err := h.temporalClient.ExecuteWorkflow(
		ctx,                       // context
		workflowOptions,           // workflow start options
		peerflow.QRepFlowWorkflow, // workflow function
		cfg,                       // workflow input
		lastPartition,             // last partition
		numPartitionsProcessed,    // number of partitions processed
	)
	if err != nil {
		return nil, fmt.Errorf("unable to start QRepFlow workflow: %w", err)
	}

	return &protos.CreateQRepFlowResponse{
		WorflowId: workflowID,
	}, nil
}

// TODO implement these
// HealthCheck(context.Context, *HealthCheckRequest) (*HealthCheckResponse, error)
// 	ShutdownFlow(context.Context, *ShutdownRequest) (*ShutdownResponse, error)
