package worker

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"temporal-lite/internal/common"
	"temporal-lite/internal/engine"
	"temporal-lite/internal/model"
	"temporal-lite/internal/store"
	"time"
)

type Worker struct {
	store      store.Store
	engine     *engine.Engine
	taskQueue  string
	maxWorkers int
	ctx        context.Context
	cancel     context.CancelFunc
	running    bool
}

func NewWorker(s store.Store, e *engine.Engine, taskQueue string, maxWorkers int) *Worker {
	ctx, cancel := context.WithCancel(context.Background())
	return &Worker{
		store:      s,
		engine:     e,
		taskQueue:  taskQueue,
		maxWorkers: maxWorkers,
		ctx:        ctx,
		cancel:     cancel,
	}
}

func (w *Worker) Start() {
	if w.running {
		return
	}
	w.running = true

	sem := make(chan struct{}, w.maxWorkers)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-w.ctx.Done():
			w.running = false
			return
		case <-ticker.C:
			activities, err := w.store.PollPendingActivities(w.ctx, w.taskQueue, 10)
			if err != nil {
				continue
			}

			for _, pa := range activities {
				select {
				case <-w.ctx.Done():
					return
				case sem <- struct{}{}:
					go func(pa *model.PendingActivity) {
						defer func() { <-sem }()
						w.executeActivity(pa)
					}(pa)
				}
			}
		}
	}
}

func (w *Worker) Stop() {
	w.cancel()
}

func (w *Worker) executeActivity(pa *model.PendingActivity) {
	fn, ok := w.engine.GetActivity(pa.ActivityType)
	if !ok {
		err := fmt.Errorf("activity type %s not registered", pa.ActivityType)
		_ = w.engine.CompleteActivity(w.ctx, pa.WorkflowID, pa.RunID, pa.ActivityID, nil, err)
		return
	}

	now := common.Now()
	_ = w.store.WithTx(w.ctx, func(tx *sql.Tx) error {
		return w.store.UpdatePendingActivity(w.ctx, tx, pa.WorkflowID, pa.RunID, pa.ActivityID, pa.Attempt, &now, &now)
	})

	heartbeatCtx, heartbeatCancel := context.WithCancel(w.ctx)
	defer heartbeatCancel()

	go w.sendHeartbeats(heartbeatCtx, pa)

	result, err := fn(w.ctx, pa.Input)
	heartbeatCancel()

	if err != nil {
		if w.shouldRetry(pa, err) {
			w.retryActivity(pa, err)
			return
		}
		_ = w.engine.CompleteActivity(w.ctx, pa.WorkflowID, pa.RunID, pa.ActivityID, nil, err)
		return
	}

	_ = w.engine.CompleteActivity(w.ctx, pa.WorkflowID, pa.RunID, pa.ActivityID, result, nil)
}

func (w *Worker) sendHeartbeats(ctx context.Context, pa *model.PendingActivity) {
	interval := 30 * time.Second
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	progress := map[string]interface{}{
		"activityId": pa.ActivityID,
		"attempt":    pa.Attempt,
		"status":     "running",
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			progressJSON := common.ToJSON(progress)
			_ = w.engine.RecordHeartbeat(w.ctx, pa.WorkflowID, pa.RunID, pa.ActivityID, progressJSON)
		}
	}
}

func (w *Worker) shouldRetry(pa *model.PendingActivity, err error) bool {
	if pa.RetryPolicy == nil {
		return false
	}

	for _, nonRetryable := range pa.RetryPolicy.NonRetryableErrors {
		if errors.Is(err, errors.New(nonRetryable)) || err.Error() == nonRetryable {
			return false
		}
	}

	return pa.Attempt < pa.RetryPolicy.MaxAttempts
}

func (w *Worker) retryActivity(pa *model.PendingActivity, lastErr error) {
	delay := w.calculateRetryDelay(pa)

	go func() {
		select {
		case <-w.ctx.Done():
			return
		case <-time.After(delay):
			now := common.Now()
			nextAttempt := pa.Attempt + 1

			_ = w.store.WithTx(w.ctx, func(tx *sql.Tx) error {
				return w.store.UpdatePendingActivity(w.ctx, tx, pa.WorkflowID, pa.RunID, pa.ActivityID, nextAttempt, nil, &now)
			})
		}
	}()
}

func (w *Worker) calculateRetryDelay(pa *model.PendingActivity) time.Duration {
	if pa.RetryPolicy == nil {
		return 1 * time.Second
	}

	rp := pa.RetryPolicy
	initial := rp.InitialInterval
	if initial == 0 {
		initial = 1 * time.Second
	}

	backoff := rp.BackoffCoefficient
	if backoff <= 1 {
		backoff = 2
	}

	delay := float64(initial) * math.Pow(backoff, float64(pa.Attempt-1))

	if rp.MaxInterval > 0 {
		maxDelay := float64(rp.MaxInterval)
		if delay > maxDelay {
			delay = maxDelay
		}
	}

	return time.Duration(delay)
}

type HeartbeatRecorder interface {
	Record(ctx context.Context, progress interface{}) error
}

type activityHeartbeatRecorder struct {
	workflowID string
	runID      string
	activityID string
	engine     *engine.Engine
}

func (r *activityHeartbeatRecorder) Record(ctx context.Context, progress interface{}) error {
	progressJSON, err := json.Marshal(progress)
	if err != nil {
		return err
	}
	return r.engine.RecordHeartbeat(ctx, r.workflowID, r.runID, r.activityID, progressJSON)
}

func NewHeartbeatRecorder(workflowID, runID, activityID string, engine *engine.Engine) HeartbeatRecorder {
	return &activityHeartbeatRecorder{
		workflowID: workflowID,
		runID:      runID,
		activityID: activityID,
		engine:     engine,
	}
}
