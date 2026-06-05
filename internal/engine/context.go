package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"temporal-lite/internal/common"
	"temporal-lite/internal/model"
	"time"
)

type ActivityResult struct {
	Result json.RawMessage
	Error  error
}

type Future struct {
	result chan ActivityResult
	once   sync.Once
}

func newFuture() *Future {
	return &Future{
		result: make(chan ActivityResult, 1),
	}
}

func (f *Future) Set(result json.RawMessage, err error) {
	f.once.Do(func() {
		f.result <- ActivityResult{Result: result, Error: err}
	})
}

func (f *Future) Get(ctx context.Context) (json.RawMessage, error) {
	select {
	case r := <-f.result:
		return r.Result, r.Error
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

type WorkflowContext struct {
	ctx          context.Context
	workflowID   string
	runID        string
	isReplaying  bool
	engine       *Engine
	replayState  *ReplayState
	currentEventID int64
	mu           sync.Mutex
	activities   map[string]*Future
	timers       map[string]*Future
	signals      map[string][]*Future
	queryHandlers map[string]QueryHandler
	version      string
	lastSignalVersion string
}

type QueryHandler func(ctx context.Context, input json.RawMessage) (json.RawMessage, error)

type ReplayState struct {
	events             []*model.Event
	nextEventIndex     int
	activityResults    map[string]ActivityResult
	timerFired         map[string]bool
	signalsReceived    []*model.SignalInfo
	signalResultIndex  map[string]int
}

func newReplayState(events []*model.Event) *ReplayState {
	return &ReplayState{
		events:            events,
		nextEventIndex:    0,
		activityResults:   make(map[string]ActivityResult),
		timerFired:        make(map[string]bool),
		signalResultIndex: make(map[string]int),
	}
}

func (rs *ReplayState) peekNextEvent() *model.Event {
	if rs.nextEventIndex >= len(rs.events) {
		return nil
	}
	return rs.events[rs.nextEventIndex]
}

func (rs *ReplayState) consumeNextEvent() *model.Event {
	if rs.nextEventIndex >= len(rs.events) {
		return nil
	}
	evt := rs.events[rs.nextEventIndex]
	rs.nextEventIndex++
	return evt
}

func (wc *WorkflowContext) WorkflowID() string {
	return wc.workflowID
}

func (wc *WorkflowContext) RunID() string {
	return wc.runID
}

func (wc *WorkflowContext) IsReplaying() bool {
	return wc.isReplaying
}

func (wc *WorkflowContext) ExecuteActivity(activityType string, input interface{}, opts model.ActivityOptions) *Future {
	fut := newFuture()
	activityID := common.GenerateActivityID()

	inputJSON := common.ToJSON(input)

	wc.mu.Lock()
	defer wc.mu.Unlock()

	if wc.isReplaying {
		for wc.replayState.peekNextEvent() != nil {
			evt := wc.replayState.peekNextEvent()
			if evt.EventType == model.EventTypeActivityScheduled {
				wc.replayState.consumeNextEvent()
			} else if evt.EventType == model.EventTypeActivityCompleted {
				var attrs model.ActivityCompletedAttrs
				_ = json.Unmarshal(evt.Attributes, &attrs)
				wc.replayState.consumeNextEvent()
				fut.Set(attrs.Result, nil)
				wc.activities[attrs.ActivityID] = fut
				return fut
			} else if evt.EventType == model.EventTypeActivityFailed {
				var attrs model.ActivityFailedAttrs
				_ = json.Unmarshal(evt.Attributes, &attrs)
				wc.replayState.consumeNextEvent()
				fut.Set(nil, errors.New(attrs.Error))
				wc.activities[attrs.ActivityID] = fut
				return fut
			} else {
				break
			}
		}
		return fut
	}

	wc.engine.scheduleActivity(wc.ctx, wc.workflowID, wc.runID, activityID, activityType, inputJSON, opts)
	wc.activities[activityID] = fut

	go func() {
		for {
			select {
			case <-wc.ctx.Done():
				return
			default:
				we, err := wc.engine.store.GetWorkflowExecution(wc.ctx, wc.workflowID, wc.runID)
				if err != nil {
					fut.Set(nil, err)
					return
				}
				if we.LastEventID > wc.currentEventID {
					events, err := wc.engine.store.GetEvents(wc.ctx, wc.workflowID, wc.runID, wc.currentEventID)
					if err != nil {
						fut.Set(nil, err)
						return
					}
					for _, evt := range events {
						wc.currentEventID = evt.EventID
						if evt.EventType == model.EventTypeActivityCompleted {
							var attrs model.ActivityCompletedAttrs
							_ = json.Unmarshal(evt.Attributes, &attrs)
							if attrs.ActivityID == activityID {
								fut.Set(attrs.Result, nil)
								return
							}
						} else if evt.EventType == model.EventTypeActivityFailed {
							var attrs model.ActivityFailedAttrs
							_ = json.Unmarshal(evt.Attributes, &attrs)
							if attrs.ActivityID == activityID {
								fut.Set(nil, errors.New(attrs.Error))
								return
							}
						}
					}
				}
				time.Sleep(100 * time.Millisecond)
			}
		}
	}()

	return fut
}

func (wc *WorkflowContext) Sleep(duration time.Duration) error {
	timerID := common.GenerateTimerID()
	fut := newFuture()

	wc.mu.Lock()
	defer wc.mu.Unlock()

	if wc.isReplaying {
		for wc.replayState.peekNextEvent() != nil {
			evt := wc.replayState.peekNextEvent()
			if evt.EventType == model.EventTypeTimerScheduled {
				wc.replayState.consumeNextEvent()
			} else if evt.EventType == model.EventTypeTimerFired {
				var attrs model.TimerFiredAttrs
				_ = json.Unmarshal(evt.Attributes, &attrs)
				wc.replayState.consumeNextEvent()
				wc.replayState.timerFired[attrs.TimerID] = true
				fut.Set(nil, nil)
				wc.timers[attrs.TimerID] = fut
				return nil
			} else {
				break
			}
		}
		fut.Set(nil, fmt.Errorf("timer replay not found"))
		return nil
	}

	wc.engine.scheduleTimer(wc.ctx, wc.workflowID, wc.runID, timerID, duration)
	wc.timers[timerID] = fut

	go func() {
		for {
			select {
			case <-wc.ctx.Done():
				return
			default:
				we, err := wc.engine.store.GetWorkflowExecution(wc.ctx, wc.workflowID, wc.runID)
				if err != nil {
					fut.Set(nil, err)
					return
				}
				if we.LastEventID > wc.currentEventID {
					events, err := wc.engine.store.GetEvents(wc.ctx, wc.workflowID, wc.runID, wc.currentEventID)
					if err != nil {
						fut.Set(nil, err)
						return
					}
					for _, evt := range events {
						wc.currentEventID = evt.EventID
						if evt.EventType == model.EventTypeTimerFired {
							var attrs model.TimerFiredAttrs
							_ = json.Unmarshal(evt.Attributes, &attrs)
							if attrs.TimerID == timerID {
								fut.Set(nil, nil)
								return
							}
						}
					}
				}
				time.Sleep(100 * time.Millisecond)
			}
		}
	}()

	_, err := fut.Get(wc.ctx)
	return err
}

func (wc *WorkflowContext) AwaitSignal(signalName string) *Future {
	fut := newFuture()

	wc.mu.Lock()
	if wc.signals == nil {
		wc.signals = make(map[string][]*Future)
	}
	wc.signals[signalName] = append(wc.signals[signalName], fut)
	wc.mu.Unlock()

	if wc.isReplaying {
		wc.mu.Lock()
		for wc.replayState.peekNextEvent() != nil {
			evt := wc.replayState.peekNextEvent()
			if evt.EventType == model.EventTypeSignalReceived {
				var attrs model.SignalReceivedAttrs
				_ = json.Unmarshal(evt.Attributes, &attrs)
				if attrs.SignalName == signalName {
					wc.replayState.consumeNextEvent()
					wc.lastSignalVersion = attrs.Version
					fut.Set(attrs.Input, nil)
					wc.mu.Unlock()
					return fut
				}
			}
			break
		}
		wc.mu.Unlock()
		return fut
	}

	go func() {
		for {
			select {
			case <-wc.ctx.Done():
				return
			default:
				signals, err := wc.engine.store.PollSignals(wc.ctx, wc.workflowID, wc.runID, 10)
				if err != nil {
					continue
				}
				for _, sig := range signals {
					if sig.SignalName == signalName {
						wc.engine.handleSignalEvent(wc.ctx, wc.workflowID, wc.runID, sig)
						fut.Set(sig.Input, nil)
						return
					}
				}
				time.Sleep(200 * time.Millisecond)
			}
		}
	}()

	return fut
}

func (wc *WorkflowContext) RegisterQuery(queryName string, handler QueryHandler) {
	wc.mu.Lock()
	defer wc.mu.Unlock()
	if wc.queryHandlers == nil {
		wc.queryHandlers = make(map[string]QueryHandler)
	}
	wc.queryHandlers[queryName] = handler
}

func (wc *WorkflowContext) Version(changeID string, minSupported, maxSupported int) int {
	if wc.isReplaying {
		for wc.replayState.peekNextEvent() != nil {
			evt := wc.replayState.peekNextEvent()
			if evt.EventType == model.EventTypeVersion {
				wc.replayState.consumeNextEvent()
				return maxSupported
			}
			break
		}
	}
	attrs := map[string]interface{}{
		"changeID":      changeID,
		"minSupported":  minSupported,
		"maxSupported":  maxSupported,
		"currentVersion": maxSupported,
	}
	wc.engine.appendEvent(wc.ctx, wc.workflowID, wc.runID, model.EventTypeVersion, attrs)
	return maxSupported
}

func (wc *WorkflowContext) GetSignalVersion() string {
	return wc.lastSignalVersion
}

func (wc *WorkflowContext) ReplayState() *ReplayState {
	return wc.replayState
}

func (wc *WorkflowContext) Context() context.Context {
	return wc.ctx
}

func (wc *WorkflowContext) GetInput() json.RawMessage {
	if wc.replayState != nil && len(wc.replayState.events) > 0 {
		var attrs model.WorkflowStartedAttrs
		if err := json.Unmarshal(wc.replayState.events[0].Attributes, &attrs); err == nil {
			return attrs.Input
		}
	}
	return nil
}
