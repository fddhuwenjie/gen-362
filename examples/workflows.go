package examples

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"temporal-lite/internal/engine"
	"temporal-lite/internal/model"
	"time"
)

type OrderInput struct {
	OrderID string  `json:"orderId"`
	UserID  string  `json:"userId"`
	Amount  float64 `json:"amount"`
	Item    string  `json:"item"`
}

type OrderResult struct {
	OrderID     string `json:"orderId"`
	Status      string `json:"status"`
	ProcessedAt string `json:"processedAt"`
}

type OrderWorkflowState struct {
	Order       *OrderInput `json:"order"`
	Status      string      `json:"status"`
	Progress    int         `json:"progress"`
	PaymentDone bool        `json:"paymentDone"`
	Shipped     bool        `json:"shipped"`
	Delivered   bool        `json:"delivered"`
	Cancelled   bool        `json:"cancelled"`
	Notes       []string    `json:"notes"`
}

func OrderWorkflow(ctx *engine.WorkflowContext) (interface{}, error) {
	state := &OrderWorkflowState{
		Status:   "CREATED",
		Progress: 0,
		Notes:    []string{},
	}

	ctx.RegisterQuery("getState", func(qCtx context.Context, input json.RawMessage) (json.RawMessage, error) {
		return json.Marshal(state)
	})

	we, err := getWorkflowInput(ctx)
	if err != nil {
		return nil, err
	}
	state.Order = we
	state.Notes = append(state.Notes, "Order created")

	v := ctx.Version("order-flow-v2", 1, 2)
	if v >= 2 {
		state.Notes = append(state.Notes, "Running v2 workflow")
	}

	retryPolicy := &model.RetryPolicy{
		InitialInterval:    1 * time.Second,
		BackoffCoefficient: 2,
		MaxInterval:        30 * time.Second,
		MaxAttempts:        5,
	}

	opts := model.ActivityOptions{
		TaskQueue:        "default",
		StartToCloseTimeout: 5 * time.Minute,
		HeartbeatTimeout: 30 * time.Second,
		RetryPolicy:      retryPolicy,
	}

	state.Status = "PAYMENT_PROCESSING"
	state.Progress = 25

	paymentFut := ctx.ExecuteActivity("processPayment", map[string]interface{}{
		"orderId": state.Order.OrderID,
		"amount":  state.Order.Amount,
		"userId":  state.Order.UserID,
	}, opts)

	paymentResult, err := paymentFut.Get(ctx.Context())
	if err != nil {
		state.Status = "FAILED"
		return nil, fmt.Errorf("payment failed: %w", err)
	}
	state.PaymentDone = true
	state.Status = "PAID"
	state.Progress = 50
	state.Notes = append(state.Notes, string(paymentResult))

	signalFut := ctx.AwaitSignal("updateOrder")
	select {
	case _ = <-func() chan interface{} {
		ch := make(chan interface{}, 1)
		go func() {
			data, err := signalFut.Get(ctx.Context())
			if err == nil {
				var update map[string]interface{}
				_ = json.Unmarshal(data, &update)
				if note, ok := update["note"].(string); ok {
					state.Notes = append(state.Notes, note)
				}
				ch <- update
			}
		}()
		return ch
	}():
	case <-time.After(2 * time.Second):
	}

	state.Status = "SHIPPING"
	state.Progress = 75

	shipFut := ctx.ExecuteActivity("shipOrder", map[string]interface{}{
		"orderId": state.Order.OrderID,
		"address": "123 Main St",
	}, opts)

	_, err = shipFut.Get(ctx.Context())
	if err != nil {
		return nil, fmt.Errorf("shipping failed: %w", err)
	}
	state.Shipped = true
	state.Status = "SHIPPED"

	if err := ctx.Sleep(3 * time.Second); err != nil {
		return nil, err
	}

	state.Status = "DELIVERED"
	state.Progress = 100
	state.Delivered = true

	return OrderResult{
		OrderID:     state.Order.OrderID,
		Status:      state.Status,
		ProcessedAt: time.Now().UTC().Format(time.RFC3339),
	}, nil
}

func SleepWorkflow(ctx *engine.WorkflowContext) (interface{}, error) {
	state := struct {
		Stage  string `json:"stage"`
		Step   int    `json:"step"`
	}{
		Stage: "START",
		Step:  0,
	}

	ctx.RegisterQuery("getState", func(qCtx context.Context, input json.RawMessage) (json.RawMessage, error) {
		return json.Marshal(state)
	})

	state.Stage = "SLEEPING_7D"
	state.Step = 1

	if err := ctx.Sleep(7 * 24 * time.Hour); err != nil {
		return nil, err
	}

	state.Stage = "WOKE_UP"
	state.Step = 2
	state.Stage = "COMPLETED"

	return map[string]interface{}{
		"status": "completed",
		"slept":  "7d",
	}, nil
}

func getWorkflowInput(ctx *engine.WorkflowContext) (*OrderInput, error) {
	input := ctx.GetInput()
	if input == nil {
		return nil, errors.New("workflow input not available")
	}

	var order OrderInput
	if err := json.Unmarshal(input, &order); err != nil {
		return nil, err
	}
	return &order, nil
}

func ProcessPayment(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
	var params map[string]interface{}
	if err := json.Unmarshal(input, &params); err != nil {
		return nil, err
	}

	orderId, _ := params["orderId"].(string)
	amount, _ := params["amount"].(float64)

	time.Sleep(500 * time.Millisecond)

	result := map[string]interface{}{
		"success":    true,
		"orderId":    orderId,
		"amount":     amount,
		"processed":  time.Now().UTC().Format(time.RFC3339),
		"externalId": fmt.Sprintf("pay_%s", orderId),
	}

	return json.Marshal(result)
}

func ShipOrder(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
	var params map[string]interface{}
	if err := json.Unmarshal(input, &params); err != nil {
		return nil, err
	}

	orderId, _ := params["orderId"].(string)

	time.Sleep(500 * time.Millisecond)

	result := map[string]interface{}{
		"success":    true,
		"orderId":    orderId,
		"trackingNo": fmt.Sprintf("TRK-%s", orderId),
		"carrier":    "FastShip",
		"shippedAt":  time.Now().UTC().Format(time.RFC3339),
	}

	return json.Marshal(result)
}

func UnreliableActivity(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
	var params map[string]interface{}
	_ = json.Unmarshal(input, &params)

	attempt, _ := params["_attempt"].(float64)
	if int(attempt) < 3 {
		return nil, errors.New("temporary failure - please retry")
	}

	return json.Marshal(map[string]interface{}{
		"success": true,
		"attempt": int(attempt),
		"message": "finally succeeded after retries",
	})
}

func RegisterWorkflows(e *engine.Engine) {
	e.RegisterWorkflow("OrderWorkflow", OrderWorkflow)
	e.RegisterWorkflow("SleepWorkflow", SleepWorkflow)
}

func RegisterActivities(e *engine.Engine) {
	e.RegisterActivity("processPayment", ProcessPayment)
	e.RegisterActivity("shipOrder", ShipOrder)
	e.RegisterActivity("unreliable", UnreliableActivity)
}
