package peerflow

import (
	"fmt"
	"strings"
	"time"

	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/shared"
	"github.com/google/uuid"
	"github.com/hashicorp/go-multierror"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const (
	CDCFlowStatusQuery     = "q-cdc-flow-status"
	maxSyncFlowsPerCDCFlow = 32
)

type CDCFlowLimits struct {
	// Number of sync flows to execute in total.
	// If 0, the number of sync flows will be continuously executed until the peer flow is cancelled.
	// This is typically non-zero for testing purposes.
	TotalSyncFlows int
	// Number of normalize flows to execute in total.
	// If 0, the number of sync flows will be continuously executed until the peer flow is cancelled.
	// This is typically non-zero for testing purposes.
	TotalNormalizeFlows int
	// Maximum number of rows in a sync flow batch.
	MaxBatchSize int
}

type CDCFlowWorkflowState struct {
	// Progress events for the peer flow.
	Progress []string
	// Accumulates status for sync flows spawned.
	SyncFlowStatuses []*model.SyncResponse
	// Accumulates status for sync flows spawned.
	NormalizeFlowStatuses []*model.NormalizeResponse
	// Current signalled state of the peer flow.
	ActiveSignal shared.CDCFlowSignal
	// SetupComplete indicates whether the peer flow setup has completed.
	SetupComplete bool
	// SnapshotComplete indicates whether the initial snapshot workflow has completed.
	SnapshotComplete bool
	// Errors encountered during child sync flow executions.
	SyncFlowErrors error
	// Errors encountered during child sync flow executions.
	NormalizeFlowErrors error
	// Global mapping of relation IDs to RelationMessages sent as a part of logical replication.
	// Needed to support schema changes.
	RelationMessageMapping *model.RelationMessageMapping
}

// returns a new empty PeerFlowState
func NewCDCFlowWorkflowState() *CDCFlowWorkflowState {
	return &CDCFlowWorkflowState{
		Progress:              []string{"started"},
		SyncFlowStatuses:      nil,
		NormalizeFlowStatuses: nil,
		ActiveSignal:          shared.NoopSignal,
		SetupComplete:         false,
		SyncFlowErrors:        nil,
		NormalizeFlowErrors:   nil,
		// WORKAROUND: empty maps are protobufed into nil maps for reasons beyond me
		RelationMessageMapping: &model.RelationMessageMapping{
			0: &protos.RelationMessage{
				RelationId:   0,
				RelationName: "protobuf_workaround",
			},
		},
	}
}

// truncate the progress and other arrays to a max of 10 elements
func (s *CDCFlowWorkflowState) TruncateProgress() {
	if len(s.Progress) > 10 {
		s.Progress = s.Progress[len(s.Progress)-10:]
	}
	if len(s.SyncFlowStatuses) > 10 {
		s.SyncFlowStatuses = s.SyncFlowStatuses[len(s.SyncFlowStatuses)-10:]
	}
	if len(s.NormalizeFlowStatuses) > 10 {
		s.NormalizeFlowStatuses = s.NormalizeFlowStatuses[len(s.NormalizeFlowStatuses)-10:]
	}

	if s.SyncFlowErrors != nil {
		fmt.Println("SyncFlowErrors: ", s.SyncFlowErrors)
		s.SyncFlowErrors = nil
	}

	if s.NormalizeFlowErrors != nil {
		fmt.Println("NormalizeFlowErrors: ", s.NormalizeFlowErrors)
		s.NormalizeFlowErrors = nil
	}
}

func (s *CDCFlowWorkflowState) SendWALHeartbeat(ctx workflow.Context, cfg *protos.FlowConnectionConfigs) error {
	walHeartbeatCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,
	})

	if err := workflow.ExecuteActivity(walHeartbeatCtx, flowable.SendWALHeartbeat, cfg).Get(ctx, nil); err != nil {
		return fmt.Errorf("failed to send WAL heartbeat: %w", err)
	}

	return nil
}

// CDCFlowWorkflowExecution represents the state for execution of a peer flow.
type CDCFlowWorkflowExecution struct {
	flowExecutionID string
	logger          log.Logger
	ctx             workflow.Context
}

// NewCDCFlowWorkflowExecution creates a new instance of PeerFlowWorkflowExecution.
func NewCDCFlowWorkflowExecution(ctx workflow.Context) *CDCFlowWorkflowExecution {
	return &CDCFlowWorkflowExecution{
		flowExecutionID: workflow.GetInfo(ctx).WorkflowExecution.ID,
		logger:          workflow.GetLogger(ctx),
		ctx:             ctx,
	}
}

func GetChildWorkflowID(
	ctx workflow.Context,
	prefix string,
	peerFlowName string,
) (string, error) {
	childWorkflowIDSideEffect := workflow.SideEffect(ctx, func(ctx workflow.Context) interface{} {
		return fmt.Sprintf("%s-%s-%s", prefix, peerFlowName, uuid.New().String())
	})

	var childWorkflowID string
	if err := childWorkflowIDSideEffect.Get(&childWorkflowID); err != nil {
		return "", fmt.Errorf("failed to get child workflow ID: %w", err)
	}

	return childWorkflowID, nil
}

// CDCFlowWorkflowResult is the result of the PeerFlowWorkflow.
type CDCFlowWorkflowResult = CDCFlowWorkflowState

func (w *CDCFlowWorkflowExecution) signalHandler(state *CDCFlowWorkflowState, v shared.CDCFlowSignal) {
	w.logger.Info("received signal - ", v)
	if v == shared.ShutdownSignal {
		w.logger.Info("received shutdown signal")
		state.ActiveSignal = v
	} else if v == shared.PauseSignal {
		w.logger.Info("received pause signal")
		if state.ActiveSignal == shared.NoopSignal {
			w.logger.Info("workflow was running, pausing it")
			state.ActiveSignal = shared.PauseSignal
		} else if state.ActiveSignal == shared.PauseSignal {
			w.logger.Info("workflow was paused, resuming it")
			state.ActiveSignal = shared.NoopSignal
		}
	}
}

func (w *CDCFlowWorkflowExecution) receiveAndHandleSignal(ctx workflow.Context, state *CDCFlowWorkflowState) {
	signalChan := workflow.GetSignalChannel(ctx, shared.CDCFlowSignalName)

	var signalVal shared.CDCFlowSignal
	ok := signalChan.ReceiveAsync(&signalVal)
	if ok {
		w.signalHandler(state, signalVal)
	}
}

func CDCFlowWorkflowWithConfig(
	ctx workflow.Context,
	cfg *protos.FlowConnectionConfigs,
	limits *CDCFlowLimits,
	state *CDCFlowWorkflowState,
) (*CDCFlowWorkflowResult, error) {
	if state == nil {
		state = NewCDCFlowWorkflowState()
	}

	if cfg == nil {
		return nil, fmt.Errorf("invalid connection configs")
	}

	w := NewCDCFlowWorkflowExecution(ctx)

	if limits.TotalSyncFlows == 0 {
		limits.TotalSyncFlows = maxSyncFlowsPerCDCFlow
	}

	// Support a Query for the current state of the peer flow.
	err := workflow.SetQueryHandler(ctx, CDCFlowStatusQuery, func(jobName string) (CDCFlowWorkflowState, error) {
		return *state, nil
	})
	if err != nil {
		return state, fmt.Errorf("failed to set `%s` query handler: %w", CDCFlowStatusQuery, err)
	}

	// we cannot skip SetupFlow if SnapshotFlow did not complete in cases where Resync is enabled
	// because Resync modifies TableMappings before Setup and also before Snapshot
	// for safety, rely on the idempotency of SetupFlow instead
	// also, no signals are being handled until the loop starts, so no PAUSE/DROP will take here.
	if !(state.SetupComplete && state.SnapshotComplete) {
		// if resync is true, alter the table name schema mapping to temporarily add
		// a suffix to the table names.
		if cfg.Resync {
			for _, mapping := range cfg.TableMappings {
				oldName := mapping.DestinationTableIdentifier
				newName := fmt.Sprintf("%s_resync", oldName)
				mapping.DestinationTableIdentifier = newName
			}
		}

		// start the SetupFlow workflow as a child workflow, and wait for it to complete
		// it should return the table schema for the source peer
		setupFlowID, err := GetChildWorkflowID(ctx, "setup-flow", cfg.FlowJobName)
		if err != nil {
			return state, err
		}
		childSetupFlowOpts := workflow.ChildWorkflowOptions{
			WorkflowID:        setupFlowID,
			ParentClosePolicy: enums.PARENT_CLOSE_POLICY_REQUEST_CANCEL,
			RetryPolicy: &temporal.RetryPolicy{
				MaximumAttempts: 20,
			},
		}
		setupFlowCtx := workflow.WithChildOptions(ctx, childSetupFlowOpts)
		setupFlowFuture := workflow.ExecuteChildWorkflow(setupFlowCtx, SetupFlowWorkflow, cfg)
		if err := setupFlowFuture.Get(setupFlowCtx, &cfg); err != nil {
			return state, fmt.Errorf("failed to execute child workflow: %w", err)
		}
		state.SetupComplete = true

		// next part of the setup is to snapshot-initial-copy and setup replication slots.
		snapshotFlowID, err := GetChildWorkflowID(ctx, "snapshot-flow", cfg.FlowJobName)
		if err != nil {
			return state, err
		}
		childSnapshotFlowOpts := workflow.ChildWorkflowOptions{
			WorkflowID:        snapshotFlowID,
			ParentClosePolicy: enums.PARENT_CLOSE_POLICY_REQUEST_CANCEL,
			RetryPolicy: &temporal.RetryPolicy{
				MaximumAttempts: 20,
			},
			TaskQueue: shared.SnapshotFlowTaskQueue,
		}
		snapshotFlowCtx := workflow.WithChildOptions(ctx, childSnapshotFlowOpts)
		snapshotFlowFuture := workflow.ExecuteChildWorkflow(snapshotFlowCtx, SnapshotFlowWorkflow, cfg)
		if err := snapshotFlowFuture.Get(snapshotFlowCtx, nil); err != nil {
			return state, fmt.Errorf("failed to execute child workflow: %w", err)
		}

		if cfg.Resync {
			renameOpts := &protos.RenameTablesInput{}
			renameOpts.FlowJobName = cfg.FlowJobName
			renameOpts.Peer = cfg.Destination
			for _, mapping := range cfg.TableMappings {
				oldName := mapping.DestinationTableIdentifier
				newName := strings.TrimSuffix(oldName, "_resync")
				renameOpts.RenameTableOptions = append(renameOpts.RenameTableOptions, &protos.RenameTableOption{
					CurrentName: oldName,
					NewName:     newName,
				})
				mapping.DestinationTableIdentifier = newName
			}

			renameTablesCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
				StartToCloseTimeout: 12 * time.Hour,
				HeartbeatTimeout:    1 * time.Hour,
			})
			renameTablesFuture := workflow.ExecuteActivity(renameTablesCtx, flowable.RenameTables, renameOpts)
			if err := renameTablesFuture.Get(renameTablesCtx, nil); err != nil {
				return state, fmt.Errorf("failed to execute rename tables activity: %w", err)
			}
		}

		state.SnapshotComplete = true
		state.Progress = append(state.Progress, "executed setup flow and snapshot flow")
	}

	syncFlowOptions := &protos.SyncFlowOptions{
		BatchSize: int32(limits.MaxBatchSize),
	}

	currentSyncFlowNum := 0

	for {
		// check and act on signals before a fresh flow starts.
		w.receiveAndHandleSignal(ctx, state)

		// check if the peer flow has been shutdown
		if state.ActiveSignal == shared.ShutdownSignal {
			w.logger.Info("peer flow has been shutdown")
			return state, nil
		}

		if state.ActiveSignal == shared.PauseSignal {
			startTime := time.Now()
			for state.ActiveSignal == shared.PauseSignal {
				err = workflow.Sleep(ctx, 1*time.Minute)
				if err != nil {
					return state, err
				}
				w.logger.Info("mirror has been paused for ", time.Since(startTime))
				w.receiveAndHandleSignal(ctx, state)
			}
			if state.ActiveSignal == shared.ShutdownSignal {
				// handling going from paused to shutdown
				continue
			}
		}

		// check if total sync flows have been completed
		// since this happens immediately after we check for signals, the case of a signal being missed
		// due to a new workflow starting is vanishingly low, but possible
		if limits.TotalSyncFlows != 0 && currentSyncFlowNum == limits.TotalSyncFlows {
			w.logger.Info("All the syncflows have completed successfully, there was a"+
				" limit on the number of syncflows to be executed: ", limits.TotalSyncFlows)
			break
		}
		currentSyncFlowNum++

		syncFlowID, err := GetChildWorkflowID(ctx, "sync-flow", cfg.FlowJobName)
		if err != nil {
			return state, err
		}

		// execute the sync flow as a child workflow
		childSyncFlowOpts := workflow.ChildWorkflowOptions{
			WorkflowID:        syncFlowID,
			ParentClosePolicy: enums.PARENT_CLOSE_POLICY_REQUEST_CANCEL,
			RetryPolicy: &temporal.RetryPolicy{
				MaximumAttempts: 20,
			},
		}
		ctx = workflow.WithChildOptions(ctx, childSyncFlowOpts)
		syncFlowOptions.RelationMessageMapping = *state.RelationMessageMapping
		childSyncFlowFuture := workflow.ExecuteChildWorkflow(
			ctx,
			SyncFlowWorkflow,
			cfg,
			syncFlowOptions,
		)

		var childSyncFlowRes *model.SyncResponse
		if err := childSyncFlowFuture.Get(ctx, &childSyncFlowRes); err != nil {
			w.logger.Error("failed to execute sync flow: ", err)
			state.SyncFlowErrors = multierror.Append(state.SyncFlowErrors, err)
		} else {
			state.SyncFlowStatuses = append(state.SyncFlowStatuses, childSyncFlowRes)
			if childSyncFlowRes != nil {
				state.RelationMessageMapping = childSyncFlowRes.RelationMessageMapping
			}
		}

		normalizeFlowID, err := GetChildWorkflowID(ctx, "normalize-flow", cfg.FlowJobName)
		if err != nil {
			return state, err
		}

		childNormalizeFlowOpts := workflow.ChildWorkflowOptions{
			WorkflowID:        normalizeFlowID,
			ParentClosePolicy: enums.PARENT_CLOSE_POLICY_REQUEST_CANCEL,
			RetryPolicy: &temporal.RetryPolicy{
				MaximumAttempts: 20,
			},
		}
		ctx = workflow.WithChildOptions(ctx, childNormalizeFlowOpts)

		var tableSchemaDeltas []*protos.TableSchemaDelta = nil
		if childSyncFlowRes != nil {
			tableSchemaDeltas = childSyncFlowRes.TableSchemaDeltas
		}

		// slightly hacky: table schema mapping is cached, so we need to manually update it if schema changes.
		if tableSchemaDeltas != nil {
			modifiedSrcTables := make([]string, 0)
			modifiedDstTables := make([]string, 0)

			for _, tableSchemaDelta := range tableSchemaDeltas {
				modifiedSrcTables = append(modifiedSrcTables, tableSchemaDelta.SrcTableName)
				modifiedDstTables = append(modifiedDstTables, tableSchemaDelta.DstTableName)
			}

			getModifiedSchemaCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
				StartToCloseTimeout: 5 * time.Minute,
			})
			getModifiedSchemaFuture := workflow.ExecuteActivity(getModifiedSchemaCtx, flowable.GetTableSchema,
				&protos.GetTableSchemaBatchInput{
					PeerConnectionConfig: cfg.Source,
					TableIdentifiers:     modifiedSrcTables,
				})

			var getModifiedSchemaRes *protos.GetTableSchemaBatchOutput
			if err := getModifiedSchemaFuture.Get(ctx, &getModifiedSchemaRes); err != nil {
				w.logger.Error("failed to execute schema update at source: ", err)
				state.SyncFlowErrors = multierror.Append(state.SyncFlowErrors, err)
			} else {
				for i := range modifiedSrcTables {
					cfg.TableNameSchemaMapping[modifiedDstTables[i]] =
						getModifiedSchemaRes.TableNameSchemaMapping[modifiedSrcTables[i]]
				}
			}
		}

		childNormalizeFlowFuture := workflow.ExecuteChildWorkflow(
			ctx,
			NormalizeFlowWorkflow,
			cfg,
		)

		selector := workflow.NewSelector(ctx)
		selector.AddFuture(childNormalizeFlowFuture, func(f workflow.Future) {
			var childNormalizeFlowRes *model.NormalizeResponse
			if err := f.Get(ctx, &childNormalizeFlowRes); err != nil {
				w.logger.Error("failed to execute normalize flow: ", err)
				state.NormalizeFlowErrors = multierror.Append(state.NormalizeFlowErrors, err)
			} else {
				state.NormalizeFlowStatuses = append(state.NormalizeFlowStatuses, childNormalizeFlowRes)
			}
		})
		selector.Select(ctx)
	}

	// send WAL heartbeat
	if err := state.SendWALHeartbeat(ctx, cfg); err != nil {
		return state, err
	}

	state.TruncateProgress()
	return nil, workflow.NewContinueAsNewError(ctx, CDCFlowWorkflowWithConfig, cfg, limits, state)
}
