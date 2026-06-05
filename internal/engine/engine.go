package engine

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"
	"temporal-lite/internal/common"
	"temporal-lite/internal/model"
	"temporal-lite/internal/store"
	"time"
)

type WorkflowFunc func(ctx *WorkflowContext) (interface{}, error)

type ActivityFunc func(ctx context.Context, input json.RawMessage) (json.RawMessage, error)

type Engine struct {
	store       store.Store
	workflows   map[string]WorkflowFunc
	activities  map[string]ActivityFunc
	workflowMu  sync.RWMutex
	activityMu  sync.RWMutex
	nextEventID map[string]int64
	eventIDMu   sync.Mutex
}

func NewEngine(s store.Store) *Engine {
	return &Engine{
		store:       s,
		workflows:   make(map[string]WorkflowFunc),
		activities:  make(map[string]ActivityFunc),
		nextEventID: make(map[string]int64),
	}
}

func (e *Engine) RegisterWorkflow(name string, fn WorkflowFunc) {
	e.workflowMu.Lock()
	defer e.workflowMu.Unlock()
	e.workflows[name] = fn
}

func (e *Engine) RegisterActivity(name string, fn ActivityFunc) {
	e.activityMu.Lock()
	defer e.activityMu.Unlock()
	e.activities[name] = fn
}

func (e *Engine) GetWorkflow(name string) (WorkflowFunc, bool) {
	e.workflowMu.RLock()
	defer e.workflowMu.RUnlock()
	fn, ok := e.workflows[name]
	return fn, ok
}

func (e *Engine) GetActivity(name string) (ActivityFunc, bool) {
	e.activityMu.RLock()
	defer e.activityMu.RUnlock()
	fn, ok := e.activities[name]
	return fn, ok
}

func (e *Engine) getNextEventID(workflowID, runID string) int64 {
	key := workflowID + ":" + runID
	e.eventIDMu.Lock()
	defer e.eventIDMu.Unlock()
	if _, ok := e.nextEventID[key]; !ok {
		we, err := e.store.GetWorkflowExecution(context.Background(), workflowID, runID)
		if err == nil {
			e.nextEventID[key] = we.LastEventID
		} else {
			e.nextEventID[key] = 1
		}
	}
	e.nextEventID[key]++
	return e.nextEventID[key]
}

func (e *Engine) appendEvent(ctx context.Context, workflowID, runID string, eventType model.EventType, attrs interface{}) (*model.Event, error) {
	var event *model.Event
	err := e.store.WithTx(ctx, func(tx *sql.Tx) error {
		eventID := e.getNextEventID(workflowID, runID)
		var err error
		event, err = e.store.AppendEvent(ctx, tx, workflowID, runID, eventID, eventType, attrs)
		return err
	})
	return event, err
}

func (e *Engine) StartWorkflow(ctx context.Context, workflowType string, input interface{}, workflowID string, version string) (*model.WorkflowExecution, error) {
	if workflowID == "" {
		workflowID = common.GenerateWorkflowID()
	}
	runID := common.GenerateRunID()
	inputJSON := common.ToJSON(input)

	we, err := e.store.CreateWorkflowExecution(ctx, workflowID, runID, workflowType, inputJSON, version)
	if err != nil {
		return nil, err
	}

	go e.ExecuteWorkflow(ctx, workflowID, runID, false)

	return we, nil
}

func (e *Engine) ExecuteWorkflow(ctx context.Context, workflowID, runID string, isReplay bool) {
	we, err := e.store.GetWorkflowExecution(ctx, workflowID, runID)
	if err != nil {
		return
	}

	fn, ok := e.GetWorkflow(we.WorkflowType)
	if !ok {
		_ = e.store.UpdateWorkflowExecution(ctx, workflowID, runID, model.WorkflowStatusFailed, we.LastEventID, nil,
			fmt.Sprintf("workflow type %s not registered", we.WorkflowType))
		return
	}

	events, err := e.store.GetAllEvents(ctx, workflowID, runID)
	if err != nil {
		return
	}

	replayState := newReplayState(events)

	wc := &WorkflowContext{
		ctx:          ctx,
		workflowID:   workflowID,
		runID:        runID,
		isReplaying:  isReplay,
		engine:       e,
		replayState:  replayState,
		currentEventID: we.LastEventID,
		activities:   make(map[string]*Future),
		timers:       make(map[string]*Future),
		signals:      make(map[string][]*Future),
		queryHandlers: make(map[string]QueryHandler),
	}

	if !isReplay {
		go e.processTimers(ctx)
	}

	result, err := fn(wc)

	if err != nil {
		attrs := model.WorkflowFailedAttrs{Error: err.Error()}
		_, _ = e.appendEvent(ctx, workflowID, runID, model.EventTypeWorkflowFailed, attrs)
		_ = e.store.UpdateWorkflowExecution(ctx, workflowID, runID, model.WorkflowStatusFailed, wc.currentEventID, nil, err.Error())
		return
	}

	resultJSON := common.ToJSON(result)
	attrs := model.WorkflowCompletedAttrs{Result: resultJSON}
	_, _ = e.appendEvent(ctx, workflowID, runID, model.EventTypeWorkflowCompleted, attrs)
	_ = e.store.UpdateWorkflowExecution(ctx, workflowID, runID, model.WorkflowStatusCompleted, wc.currentEventID, resultJSON, "")
}

func (e *Engine) ReplayWorkflow(ctx context.Context, workflowID, runID string) error {
	e.ExecuteWorkflow(ctx, workflowID, runID, true)
	return nil
}

func (e *Engine) ExecuteQuery(ctx context.Context, workflowID, runID, queryName string, input json.RawMessage) (json.RawMessage, error) {
	we, err := e.store.GetWorkflowExecution(ctx, workflowID, runID)
	if err != nil {
		return nil, err
	}

	fn, ok := e.GetWorkflow(we.WorkflowType)
	if !ok {
		return nil, fmt.Errorf("workflow type %s not registered", we.WorkflowType)
	}

	events, err := e.store.GetAllEvents(ctx, workflowID, runID)
	if err != nil {
		return nil, err
	}

	replayState := newReplayState(events)

	wc := &WorkflowContext{
		ctx:          ctx,
		workflowID:   workflowID,
		runID:        runID,
		isReplaying:  true,
		engine:       e,
		replayState:  replayState,
		currentEventID: we.LastEventID,
		activities:   make(map[string]*Future),
		timers:       make(map[string]*Future),
		signals:      make(map[string][]*Future),
		queryHandlers: make(map[string]QueryHandler),
	}

	_, _ = fn(wc)

	wc.mu.Lock()
	handler, ok := wc.queryHandlers[queryName]
	wc.mu.Unlock()

	if !ok {
		return nil, fmt.Errorf("query handler %s not registered", queryName)
	}

	return handler(ctx, input)
}

func (e *Engine) scheduleActivity(ctx context.Context, workflowID, runID, activityID, activityType string, input json.RawMessage, opts model.ActivityOptions) {
	attrs := model.ActivityScheduledAttrs{
		ActivityID:   activityID,
		ActivityType: activityType,
		Input:        input,
		Options:      opts,
	}
	_, _ = e.appendEvent(ctx, workflowID, runID, model.EventTypeActivityScheduled, attrs)

	taskQueue := opts.TaskQueue
	if taskQueue == "" {
		taskQueue = "default"
	}

	pa := &model.PendingActivity{
		WorkflowID:  workflowID,
		RunID:       runID,
		ActivityID:  activityID,
		ActivityType: activityType,
		Input:       input,
		Attempt:     1,
		ScheduledAt: common.Now(),
		RetryPolicy: opts.RetryPolicy,
		TaskQueue:   taskQueue,
	}

	_ = e.store.WithTx(ctx, func(tx *sql.Tx) error {
		return e.store.CreatePendingActivity(ctx, tx, pa)
	})
}

func (e *Engine) scheduleTimer(ctx context.Context, workflowID, runID, timerID string, duration time.Duration) {
	fireTime := common.Now().Add(duration)
	attrs := model.TimerScheduledAttrs{
		TimerID:  timerID,
		Duration: duration,
		FireTime: fireTime,
	}
	_, _ = e.appendEvent(ctx, workflowID, runID, model.EventTypeTimerScheduled, attrs)

	pt := &model.PendingTimer{
		WorkflowID: workflowID,
		RunID:      runID,
		TimerID:    timerID,
		FireTime:   fireTime,
		HandlerID:  timerID,
		Fired:      false,
	}

	_ = e.store.WithTx(ctx, func(tx *sql.Tx) error {
		return e.store.CreatePendingTimer(ctx, tx, pt)
	})
}

func (e *Engine) handleSignalEvent(ctx context.Context, workflowID, runID string, sig *model.SignalInfo) {
	attrs := model.SignalReceivedAttrs{
		SignalName: sig.SignalName,
		Input:      sig.Input,
		Version:    "v1",
	}
	_, _ = e.appendEvent(ctx, workflowID, runID, model.EventTypeSignalReceived, attrs)
}

func (e *Engine) processTimers(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			now := common.Now()
			timers, err := e.store.GetPendingTimersToFire(ctx, now, 100)
			if err != nil {
				continue
			}
			for _, pt := range timers {
				func(pt *model.PendingTimer) {
					_ = e.store.WithTx(ctx, func(tx *sql.Tx) error {
						attrs := model.TimerFiredAttrs{TimerID: pt.TimerID}
						_, err := e.store.AppendEvent(ctx, tx, pt.WorkflowID, pt.RunID,
							e.getNextEventID(pt.WorkflowID, pt.RunID), model.EventTypeTimerFired, attrs)
						if err != nil {
							return err
						}
						return e.store.UpdateTimerFired(ctx, tx, pt.WorkflowID, pt.RunID, pt.TimerID)
					})
				}(pt)
			}
		}
	}
}

func (e *Engine) CompleteActivity(ctx context.Context, workflowID, runID, activityID string, result json.RawMessage, execErr error) error {
	var eventType model.EventType
	var attrs interface{}

	if execErr != nil {
		eventType = model.EventTypeActivityFailed
		attrs = model.ActivityFailedAttrs{
			ActivityID: activityID,
			Error:      execErr.Error(),
			Retryable:  true,
		}
	} else {
		eventType = model.EventTypeActivityCompleted
		attrs = model.ActivityCompletedAttrs{
			ActivityID: activityID,
			Result:     result,
		}
	}

	_, err := e.appendEvent(ctx, workflowID, runID, eventType, attrs)
	if err != nil {
		return err
	}

	_ = e.store.WithTx(ctx, func(tx *sql.Tx) error {
		return e.store.DeletePendingActivity(ctx, tx, workflowID, runID, activityID)
	})

	return nil
}

func (e *Engine) RecordHeartbeat(ctx context.Context, workflowID, runID, activityID string, progress json.RawMessage) error {
	attrs := model.ActivityHeartbeatAttrs{
		ActivityID: activityID,
		Progress:   progress,
	}
	_, _ = e.appendEvent(ctx, workflowID, runID, model.EventTypeActivityHeartbeat, attrs)
	return e.store.RecordHeartbeat(ctx, workflowID, runID, activityID, progress)
}

func (e *Engine) SendSignal(ctx context.Context, workflowID, runID, signalName string, input interface{}, version string) error {
	inputJSON := common.ToJSON(input)
	return e.store.EnqueueSignal(ctx, workflowID, runID, signalName, inputJSON, version)
}

func (e *Engine) ReplayWorkflowForDebug(ctx context.Context, workflowID, runID string, replayFn func(ctx *WorkflowContext) error) error {
	we, err := e.store.GetWorkflowExecution(ctx, workflowID, runID)
	if err != nil {
		return err
	}

	events, err := e.store.GetAllEvents(ctx, workflowID, runID)
	if err != nil {
		return err
	}

	replayState := newReplayState(events)

	wc := &WorkflowContext{
		ctx:          ctx,
		workflowID:   workflowID,
		runID:        runID,
		isReplaying:  true,
		engine:       e,
		replayState:  replayState,
		currentEventID: we.LastEventID,
		activities:   make(map[string]*Future),
		timers:       make(map[string]*Future),
		signals:      make(map[string][]*Future),
		queryHandlers: make(map[string]QueryHandler),
	}

	return replayFn(wc)
}
