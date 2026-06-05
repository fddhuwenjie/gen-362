package model

import (
	"encoding/json"
	"time"
)

type EventType string

const (
	EventTypeWorkflowStarted       EventType = "WORKFLOW_STARTED"
	EventTypeActivityScheduled     EventType = "ACTIVITY_SCHEDULED"
	EventTypeActivityStarted       EventType = "ACTIVITY_STARTED"
	EventTypeActivityCompleted     EventType = "ACTIVITY_COMPLETED"
	EventTypeActivityFailed        EventType = "ACTIVITY_FAILED"
	EventTypeActivityHeartbeat     EventType = "ACTIVITY_HEARTBEAT"
	EventTypeTimerScheduled        EventType = "TIMER_SCHEDULED"
	EventTypeTimerFired            EventType = "TIMER_FIRED"
	EventTypeSignalReceived        EventType = "SIGNAL_RECEIVED"
	EventTypeWorkflowCompleted     EventType = "WORKFLOW_COMPLETED"
	EventTypeWorkflowFailed        EventType = "WORKFLOW_FAILED"
	EventTypeMarker                EventType = "MARKER"
	EventTypeSideEffect            EventType = "SIDE_EFFECT"
	EventTypeVersion               EventType = "VERSION"
)

type WorkflowStatus string

const (
	WorkflowStatusRunning   WorkflowStatus = "RUNNING"
	WorkflowStatusCompleted WorkflowStatus = "COMPLETED"
	WorkflowStatusFailed    WorkflowStatus = "FAILED"
)

type RetryPolicy struct {
	InitialInterval time.Duration `json:"initialInterval"`
	BackoffCoefficient float64    `json:"backoffCoefficient"`
	MaxInterval     time.Duration `json:"maxInterval"`
	MaxAttempts     int           `json:"maxAttempts"`
	NonRetryableErrors []string   `json:"nonRetryableErrors"`
}

type ActivityOptions struct {
	TaskQueue    string        `json:"taskQueue"`
	ScheduleToCloseTimeout time.Duration `json:"scheduleToCloseTimeout"`
	StartToCloseTimeout time.Duration `json:"startToCloseTimeout"`
	HeartbeatTimeout time.Duration `json:"heartbeatTimeout"`
	RetryPolicy  *RetryPolicy  `json:"retryPolicy,omitempty"`
}

type Event struct {
	ID            int64           `json:"id"`
	EventID       int64           `json:"eventId"`
	WorkflowID    string          `json:"workflowId"`
	RunID         string          `json:"runId"`
	EventType     EventType       `json:"eventType"`
	Timestamp     time.Time       `json:"timestamp"`
	Attributes    json.RawMessage `json:"attributes"`
}

type WorkflowExecution struct {
	ID            int64            `json:"id"`
	WorkflowID    string           `json:"workflowId"`
	RunID         string           `json:"runId"`
	WorkflowType  string           `json:"workflowType"`
	Status        WorkflowStatus   `json:"status"`
	LastEventID   int64            `json:"lastEventId"`
	Input         json.RawMessage  `json:"input"`
	Result        json.RawMessage  `json:"result,omitempty"`
	Error         string           `json:"error,omitempty"`
	CreatedAt     time.Time        `json:"createdAt"`
	UpdatedAt     time.Time        `json:"updatedAt"`
	CompletedAt   *time.Time       `json:"completedAt,omitempty"`
}

type ActivityInfo struct {
	ActivityID    string          `json:"activityId"`
	ActivityType  string          `json:"activityType"`
	Input         json.RawMessage `json:"input"`
	Result        json.RawMessage `json:"result,omitempty"`
	Error         string          `json:"error,omitempty"`
	Attempt       int             `json:"attempt"`
	State         string          `json:"state"`
	LastHeartbeat time.Time       `json:"lastHeartbeat"`
	Progress      json.RawMessage `json:"progress,omitempty"`
}

type TimerInfo struct {
	TimerID       string    `json:"timerId"`
	FireTime      time.Time `json:"fireTime"`
	HandlerID     string    `json:"handlerId"`
	Fired         bool      `json:"fired"`
}

type SignalInfo struct {
	SignalName    string          `json:"signalName"`
	Input         json.RawMessage `json:"input"`
	ReceivedAt    time.Time       `json:"receivedAt"`
	Handled       bool            `json:"handled"`
}

type PendingActivity struct {
	ID             int64           `json:"id"`
	WorkflowID     string          `json:"workflowId"`
	RunID          string          `json:"runId"`
	ActivityID     string          `json:"activityId"`
	ActivityType   string          `json:"activityType"`
	Input          json.RawMessage `json:"input"`
	Attempt        int             `json:"attempt"`
	ScheduledAt    time.Time       `json:"scheduledAt"`
	StartedAt      *time.Time      `json:"startedAt"`
	RetryPolicy    *RetryPolicy    `json:"retryPolicy"`
	LastAttemptAt  *time.Time      `json:"lastAttemptAt"`
	TaskQueue      string          `json:"taskQueue"`
}

type PendingTimer struct {
	ID          int64     `json:"id"`
	WorkflowID  string    `json:"workflowId"`
	RunID       string    `json:"runId"`
	TimerID     string    `json:"timerId"`
	FireTime    time.Time `json:"fireTime"`
	HandlerID   string    `json:"handlerId"`
	Fired       bool      `json:"fired"`
}

type WorkflowStartedAttrs struct {
	WorkflowType string          `json:"workflowType"`
	Input        json.RawMessage `json:"input"`
	Version      string          `json:"version"`
}

type ActivityScheduledAttrs struct {
	ActivityID   string          `json:"activityId"`
	ActivityType string          `json:"activityType"`
	Input        json.RawMessage `json:"input"`
	Options      ActivityOptions `json:"options"`
}

type ActivityCompletedAttrs struct {
	ActivityID string          `json:"activityId"`
	Result     json.RawMessage `json:"result"`
}

type ActivityFailedAttrs struct {
	ActivityID string `json:"activityId"`
	Error      string `json:"error"`
	Retryable  bool   `json:"retryable"`
}

type ActivityHeartbeatAttrs struct {
	ActivityID string          `json:"activityId"`
	Progress   json.RawMessage `json:"progress"`
}

type TimerScheduledAttrs struct {
	TimerID  string        `json:"timerId"`
	Duration time.Duration `json:"duration"`
	FireTime time.Time     `json:"fireTime"`
}

type TimerFiredAttrs struct {
	TimerID string `json:"timerId"`
}

type SignalReceivedAttrs struct {
	SignalName string          `json:"signalName"`
	Input      json.RawMessage `json:"input"`
	Version    string          `json:"version"`
}

type WorkflowCompletedAttrs struct {
	Result json.RawMessage `json:"result"`
}

type WorkflowFailedAttrs struct {
	Error string `json:"error"`
}
