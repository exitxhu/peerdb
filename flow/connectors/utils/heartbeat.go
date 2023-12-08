package utils

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"go.temporal.io/sdk/activity"
)

func HeartbeatRoutine(
	ctx context.Context,
	interval time.Duration,
	message func() string,
) chan struct{} {
	counter := 1
	shutdown := make(chan struct{})
	go func() {
		for {
			msg := fmt.Sprintf("heartbeat #%d: %s", counter, message())
			RecordHeartbeatWithRecover(ctx, msg)
			counter += 1
			to := time.After(interval)
			select {
			case <-shutdown:
				return
			case <-to:
			}
		}
	}()
	return shutdown
}

// if the functions are being called outside the context of a Temporal workflow,
// activity.RecordHeartbeat panics, this is a bandaid for that.
func RecordHeartbeatWithRecover(ctx context.Context, details ...interface{}) {
	defer func() {
		if r := recover(); r != nil {
			slog.Warn("ignoring panic from activity.RecordHeartbeat")
			slog.Warn("this can happen when function is invoked outside of a Temporal workflow")
		}
	}()
	activity.RecordHeartbeat(ctx, details...)
}
