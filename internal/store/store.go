package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	_ "github.com/lib/pq"
	"temporal-lite/internal/model"
	"time"
)

type Store interface {
	Close() error
	WithTx(ctx context.Context, fn func(tx *sql.Tx) error) error
	CreateWorkflowExecution(ctx context.Context, workflowID, runID, workflowType string, input json.RawMessage, version string) (*model.WorkflowExecution, error)
	UpdateWorkflowExecution(ctx context.Context, workflowID, runID string, status model.WorkflowStatus, lastEventID int64, result json.RawMessage, errStr string) error
	GetWorkflowExecution(ctx context.Context, workflowID, runID string) (*model.WorkflowExecution, error)
	ListWorkflowExecutions(ctx context.Context, status model.WorkflowStatus, limit int) ([]*model.WorkflowExecution, error)
	AppendEvent(ctx context.Context, tx *sql.Tx, workflowID, runID string, eventID int64, eventType model.EventType, attrs interface{}) (*model.Event, error)
	GetEvents(ctx context.Context, workflowID, runID string, fromEventID int64) ([]*model.Event, error)
	GetAllEvents(ctx context.Context, workflowID, runID string) ([]*model.Event, error)
	CreatePendingActivity(ctx context.Context, tx *sql.Tx, pa *model.PendingActivity) error
	UpdatePendingActivity(ctx context.Context, tx *sql.Tx, workflowID, runID, activityID string, attempt int, startedAt *time.Time, lastAttemptAt *time.Time) error
	DeletePendingActivity(ctx context.Context, tx *sql.Tx, workflowID, runID, activityID string) error
	PollPendingActivities(ctx context.Context, taskQueue string, limit int) ([]*model.PendingActivity, error)
	GetPendingActivity(ctx context.Context, workflowID, runID, activityID string) (*model.PendingActivity, error)
	CreatePendingTimer(ctx context.Context, tx *sql.Tx, pt *model.PendingTimer) error
	UpdateTimerFired(ctx context.Context, tx *sql.Tx, workflowID, runID, timerID string) error
	GetPendingTimersToFire(ctx context.Context, now time.Time, limit int) ([]*model.PendingTimer, error)
	GetPendingTimers(ctx context.Context, workflowID, runID string) ([]*model.PendingTimer, error)
	RecordHeartbeat(ctx context.Context, workflowID, runID, activityID string, progress json.RawMessage) error
	GetLastHeartbeat(ctx context.Context, workflowID, runID, activityID string) (*time.Time, json.RawMessage, error)
	EnqueueSignal(ctx context.Context, workflowID, runID, signalName string, input json.RawMessage, version string) error
	PollSignals(ctx context.Context, workflowID, runID string, limit int) ([]*model.SignalInfo, error)
	MarkSignalHandled(ctx context.Context, tx *sql.Tx, id int64) error
	GetUnhandledSignals(ctx context.Context, workflowID, runID string) ([]*model.SignalInfo, error)
}

type postgresStore struct {
	db *sql.DB
}

func NewPostgresStore(connStr string) (Store, error) {
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("open db: %w", err)
	}
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("ping db: %w", err)
	}
	return &postgresStore{db: db}, nil
}

func (s *postgresStore) Close() error {
	return s.db.Close()
}

func (s *postgresStore) WithTx(ctx context.Context, fn func(tx *sql.Tx) error) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if p := recover(); p != nil {
			_ = tx.Rollback()
			panic(p)
		}
	}()
	if err := fn(tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

func (s *postgresStore) CreateWorkflowExecution(ctx context.Context, workflowID, runID, workflowType string, input json.RawMessage, version string) (*model.WorkflowExecution, error) {
	startedAttrs := model.WorkflowStartedAttrs{
		WorkflowType: workflowType,
		Input:        input,
		Version:      version,
	}
	attrsJSON, _ := json.Marshal(startedAttrs)

	var we model.WorkflowExecution
	err := s.WithTx(ctx, func(tx *sql.Tx) error {
		err := tx.QueryRowContext(ctx, `
			INSERT INTO workflow_executions (workflow_id, run_id, workflow_type, status, input, created_at, updated_at)
			VALUES ($1, $2, $3, $4, $5, NOW(), NOW())
			RETURNING id, workflow_id, run_id, workflow_type, status, last_event_id, input, created_at, updated_at
		`, workflowID, runID, workflowType, model.WorkflowStatusRunning, input).Scan(
			&we.ID, &we.WorkflowID, &we.RunID, &we.WorkflowType, &we.Status, &we.LastEventID, &we.Input, &we.CreatedAt, &we.UpdatedAt,
		)
		if err != nil {
			return err
		}

		_, err = s.AppendEvent(ctx, tx, workflowID, runID, 1, model.EventTypeWorkflowStarted, attrsJSON)
		return err
	})
	if err != nil {
		return nil, err
	}
	return &we, nil
}

func (s *postgresStore) UpdateWorkflowExecution(ctx context.Context, workflowID, runID string, status model.WorkflowStatus, lastEventID int64, result json.RawMessage, errStr string) error {
	query := `
		UPDATE workflow_executions
		SET status = $1, last_event_id = $2, updated_at = NOW()
	`
	args := []interface{}{status, lastEventID}
	argIdx := 3

	if result != nil {
		query += fmt.Sprintf(", result = $%d", argIdx)
		args = append(args, result)
		argIdx++
	}
	if errStr != "" {
		query += fmt.Sprintf(", error = $%d", argIdx)
		args = append(args, errStr)
		argIdx++
	}
	if status == model.WorkflowStatusCompleted || status == model.WorkflowStatusFailed {
		query += fmt.Sprintf(", completed_at = NOW()")
	}

	query += " WHERE workflow_id = $" + fmt.Sprint(argIdx) + " AND run_id = $" + fmt.Sprint(argIdx+1)
	args = append(args, workflowID, runID)

	_, err := s.db.ExecContext(ctx, query, args...)
	return err
}

func (s *postgresStore) GetWorkflowExecution(ctx context.Context, workflowID, runID string) (*model.WorkflowExecution, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT id, workflow_id, run_id, workflow_type, status, last_event_id, input, result, error, created_at, updated_at, completed_at
		FROM workflow_executions
		WHERE workflow_id = $1 AND run_id = $2
	`, workflowID, runID)

	var we model.WorkflowExecution
	err := row.Scan(&we.ID, &we.WorkflowID, &we.RunID, &we.WorkflowType, &we.Status, &we.LastEventID,
		&we.Input, &we.Result, &we.Error, &we.CreatedAt, &we.UpdatedAt, &we.CompletedAt)
	if err != nil {
		return nil, err
	}
	return &we, nil
}

func (s *postgresStore) ListWorkflowExecutions(ctx context.Context, status model.WorkflowStatus, limit int) ([]*model.WorkflowExecution, error) {
	query := `
		SELECT id, workflow_id, run_id, workflow_type, status, last_event_id, input, result, error, created_at, updated_at, completed_at
		FROM workflow_executions
	`
	args := []interface{}{}
	if status != "" {
		query += " WHERE status = $1"
		args = append(args, status)
	}
	query += " ORDER BY created_at DESC LIMIT $" + fmt.Sprint(len(args)+1)
	args = append(args, limit)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []*model.WorkflowExecution
	for rows.Next() {
		var we model.WorkflowExecution
		err := rows.Scan(&we.ID, &we.WorkflowID, &we.RunID, &we.WorkflowType, &we.Status, &we.LastEventID,
			&we.Input, &we.Result, &we.Error, &we.CreatedAt, &we.UpdatedAt, &we.CompletedAt)
		if err != nil {
			return nil, err
		}
		results = append(results, &we)
	}
	return results, nil
}

func (s *postgresStore) AppendEvent(ctx context.Context, tx *sql.Tx, workflowID, runID string, eventID int64, eventType model.EventType, attrs interface{}) (*model.Event, error) {
	var attrsJSON json.RawMessage
	switch v := attrs.(type) {
	case json.RawMessage:
		attrsJSON = v
	default:
		b, err := json.Marshal(attrs)
		if err != nil {
			return nil, err
		}
		attrsJSON = b
	}

	row := tx.QueryRowContext(ctx, `
		INSERT INTO workflow_events (event_id, workflow_id, run_id, event_type, timestamp, attributes)
		VALUES ($1, $2, $3, $4, NOW(), $5)
		RETURNING id, event_id, workflow_id, run_id, event_type, timestamp, attributes
	`, eventID, workflowID, runID, eventType, attrsJSON)

	var evt model.Event
	err := row.Scan(&evt.ID, &evt.EventID, &evt.WorkflowID, &evt.RunID, &evt.EventType, &evt.Timestamp, &evt.Attributes)
	if err != nil {
		return nil, err
	}

	_, err = tx.ExecContext(ctx, `
		UPDATE workflow_executions
		SET last_event_id = $1, updated_at = NOW()
		WHERE workflow_id = $2 AND run_id = $3
	`, eventID, workflowID, runID)

	return &evt, err
}

func (s *postgresStore) GetEvents(ctx context.Context, workflowID, runID string, fromEventID int64) ([]*model.Event, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, event_id, workflow_id, run_id, event_type, timestamp, attributes
		FROM workflow_events
		WHERE workflow_id = $1 AND run_id = $2 AND event_id > $3
		ORDER BY event_id ASC
	`, workflowID, runID, fromEventID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []*model.Event
	for rows.Next() {
		var evt model.Event
		err := rows.Scan(&evt.ID, &evt.EventID, &evt.WorkflowID, &evt.RunID, &evt.EventType, &evt.Timestamp, &evt.Attributes)
		if err != nil {
			return nil, err
		}
		events = append(events, &evt)
	}
	return events, nil
}

func (s *postgresStore) GetAllEvents(ctx context.Context, workflowID, runID string) ([]*model.Event, error) {
	return s.GetEvents(ctx, workflowID, runID, 0)
}

func (s *postgresStore) CreatePendingActivity(ctx context.Context, tx *sql.Tx, pa *model.PendingActivity) error {
	rpJSON, _ := json.Marshal(pa.RetryPolicy)
	_, err := tx.ExecContext(ctx, `
		INSERT INTO pending_activities (workflow_id, run_id, activity_id, activity_type, input, attempt, scheduled_at, retry_policy, task_queue)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
	`, pa.WorkflowID, pa.RunID, pa.ActivityID, pa.ActivityType, pa.Input, pa.Attempt, pa.ScheduledAt, rpJSON, pa.TaskQueue)
	return err
}

func (s *postgresStore) UpdatePendingActivity(ctx context.Context, tx *sql.Tx, workflowID, runID, activityID string, attempt int, startedAt *time.Time, lastAttemptAt *time.Time) error {
	_, err := tx.ExecContext(ctx, `
		UPDATE pending_activities
		SET attempt = $1, started_at = $2, last_attempt_at = $3
		WHERE workflow_id = $4 AND run_id = $5 AND activity_id = $6
	`, attempt, startedAt, lastAttemptAt, workflowID, runID, activityID)
	return err
}

func (s *postgresStore) DeletePendingActivity(ctx context.Context, tx *sql.Tx, workflowID, runID, activityID string) error {
	_, err := tx.ExecContext(ctx, `
		DELETE FROM pending_activities
		WHERE workflow_id = $1 AND run_id = $2 AND activity_id = $3
	`, workflowID, runID, activityID)
	return err
}

func (s *postgresStore) PollPendingActivities(ctx context.Context, taskQueue string, limit int) ([]*model.PendingActivity, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, workflow_id, run_id, activity_id, activity_type, input, attempt, scheduled_at, started_at, retry_policy, last_attempt_at, task_queue
		FROM pending_activities
		WHERE task_queue = $1 AND (started_at IS NULL OR last_attempt_at < NOW() - INTERVAL '1 minute')
		ORDER BY scheduled_at ASC
		LIMIT $2
		FOR UPDATE SKIP LOCKED
	`, taskQueue, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []*model.PendingActivity
	for rows.Next() {
		var pa model.PendingActivity
		var rpJSON json.RawMessage
		err := rows.Scan(&pa.ID, &pa.WorkflowID, &pa.RunID, &pa.ActivityID, &pa.ActivityType, &pa.Input, &pa.Attempt,
			&pa.ScheduledAt, &pa.StartedAt, &rpJSON, &pa.LastAttemptAt, &pa.TaskQueue)
		if err != nil {
			return nil, err
		}
		if rpJSON != nil {
			_ = json.Unmarshal(rpJSON, &pa.RetryPolicy)
		}
		results = append(results, &pa)
	}
	return results, nil
}

func (s *postgresStore) GetPendingActivity(ctx context.Context, workflowID, runID, activityID string) (*model.PendingActivity, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT id, workflow_id, run_id, activity_id, activity_type, input, attempt, scheduled_at, started_at, retry_policy, last_attempt_at, task_queue
		FROM pending_activities
		WHERE workflow_id = $1 AND run_id = $2 AND activity_id = $3
	`, workflowID, runID, activityID)

	var pa model.PendingActivity
	var rpJSON json.RawMessage
	err := row.Scan(&pa.ID, &pa.WorkflowID, &pa.RunID, &pa.ActivityID, &pa.ActivityType, &pa.Input, &pa.Attempt,
		&pa.ScheduledAt, &pa.StartedAt, &rpJSON, &pa.LastAttemptAt, &pa.TaskQueue)
	if err != nil {
		return nil, err
	}
	if rpJSON != nil {
		_ = json.Unmarshal(rpJSON, &pa.RetryPolicy)
	}
	return &pa, nil
}

func (s *postgresStore) CreatePendingTimer(ctx context.Context, tx *sql.Tx, pt *model.PendingTimer) error {
	_, err := tx.ExecContext(ctx, `
		INSERT INTO pending_timers (workflow_id, run_id, timer_id, fire_time, handler_id)
		VALUES ($1, $2, $3, $4, $5)
	`, pt.WorkflowID, pt.RunID, pt.TimerID, pt.FireTime, pt.HandlerID)
	return err
}

func (s *postgresStore) UpdateTimerFired(ctx context.Context, tx *sql.Tx, workflowID, runID, timerID string) error {
	_, err := tx.ExecContext(ctx, `
		UPDATE pending_timers
		SET fired = TRUE
		WHERE workflow_id = $1 AND run_id = $2 AND timer_id = $3
	`, workflowID, runID, timerID)
	return err
}

func (s *postgresStore) GetPendingTimersToFire(ctx context.Context, now time.Time, limit int) ([]*model.PendingTimer, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, workflow_id, run_id, timer_id, fire_time, handler_id, fired
		FROM pending_timers
		WHERE fire_time <= $1 AND fired = FALSE
		ORDER BY fire_time ASC
		LIMIT $2
	`, now, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []*model.PendingTimer
	for rows.Next() {
		var pt model.PendingTimer
		err := rows.Scan(&pt.ID, &pt.WorkflowID, &pt.RunID, &pt.TimerID, &pt.FireTime, &pt.HandlerID, &pt.Fired)
		if err != nil {
			return nil, err
		}
		results = append(results, &pt)
	}
	return results, nil
}

func (s *postgresStore) GetPendingTimers(ctx context.Context, workflowID, runID string) ([]*model.PendingTimer, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, workflow_id, run_id, timer_id, fire_time, handler_id, fired
		FROM pending_timers
		WHERE workflow_id = $1 AND run_id = $2
		ORDER BY fire_time ASC
	`, workflowID, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []*model.PendingTimer
	for rows.Next() {
		var pt model.PendingTimer
		err := rows.Scan(&pt.ID, &pt.WorkflowID, &pt.RunID, &pt.TimerID, &pt.FireTime, &pt.HandlerID, &pt.Fired)
		if err != nil {
			return nil, err
		}
		results = append(results, &pt)
	}
	return results, nil
}

func (s *postgresStore) RecordHeartbeat(ctx context.Context, workflowID, runID, activityID string, progress json.RawMessage) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO activity_heartbeats (workflow_id, run_id, activity_id, progress)
		VALUES ($1, $2, $3, $4)
	`, workflowID, runID, activityID, progress)
	return err
}

func (s *postgresStore) GetLastHeartbeat(ctx context.Context, workflowID, runID, activityID string) (*time.Time, json.RawMessage, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT created_at, progress
		FROM activity_heartbeats
		WHERE workflow_id = $1 AND run_id = $2 AND activity_id = $3
		ORDER BY created_at DESC
		LIMIT 1
	`, workflowID, runID, activityID)

	var createdAt time.Time
	var progress json.RawMessage
	err := row.Scan(&createdAt, &progress)
	if err != nil {
		return nil, nil, err
	}
	return &createdAt, progress, nil
}

func (s *postgresStore) EnqueueSignal(ctx context.Context, workflowID, runID, signalName string, input json.RawMessage, version string) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO signal_queue (workflow_id, run_id, signal_name, input, version)
		VALUES ($1, $2, $3, $4, $5)
	`, workflowID, runID, signalName, input, version)
	return err
}

func (s *postgresStore) PollSignals(ctx context.Context, workflowID, runID string, limit int) ([]*model.SignalInfo, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, signal_name, input, received_at
		FROM signal_queue
		WHERE workflow_id = $1 AND run_id = $2 AND handled = FALSE
		ORDER BY received_at ASC
		LIMIT $3
	`, workflowID, runID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []*model.SignalInfo
	for rows.Next() {
		var si model.SignalInfo
		var id int64
		err := rows.Scan(&id, &si.SignalName, &si.Input, &si.ReceivedAt)
		if err != nil {
			return nil, err
		}
		results = append(results, &si)
	}
	return results, nil
}

func (s *postgresStore) MarkSignalHandled(ctx context.Context, tx *sql.Tx, id int64) error {
	_, err := tx.ExecContext(ctx, `
		UPDATE signal_queue
		SET handled = TRUE
		WHERE id = $1
	`, id)
	return err
}

func (s *postgresStore) GetUnhandledSignals(ctx context.Context, workflowID, runID string) ([]*model.SignalInfo, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, signal_name, input, received_at
		FROM signal_queue
		WHERE workflow_id = $1 AND run_id = $2 AND handled = FALSE
		ORDER BY received_at ASC
	`, workflowID, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []*model.SignalInfo
	for rows.Next() {
		var si model.SignalInfo
		var id int64
		err := rows.Scan(&id, &si.SignalName, &si.Input, &si.ReceivedAt)
		if err != nil {
			return nil, err
		}
		results = append(results, &si)
	}
	return results, nil
}
