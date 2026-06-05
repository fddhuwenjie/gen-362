# Temporal Lite

A lightweight persistent workflow engine inspired by Temporal, built with Go and PostgreSQL.

## Features

1. **Workflow as Code**: Write workflows as regular Go functions that look synchronous
2. **Event Sourcing Replay**: Host process crash? Resume from the last event position exactly
3. **Activity with Retry**: Remote activities automatically retry based on retryPolicy
4. **Heartbeat Progress**: Activities report heartbeat to prevent worker staleness
5. **Persistent Timers**: `workflow.Sleep(7d)` persists to disk, survives restarts
6. **Signals & Queries**: Send signals to modify state, query current progress with versioning
7. **CLI Replay Tool**: Replay historical events locally to debug unreproducible bugs

## Quick Start

### Using Docker

```bash
docker-compose up -d
```

The server will be available at `http://localhost:8132`

### Build from Source

```bash
go mod download
go build -o bin/temporal-lite-server ./cmd/server
go build -o bin/temporal-lite ./cmd/cli
```

## API Endpoints

- `POST /api/workflow/start` - Start a new workflow
- `POST /api/workflow/signal?workflowId=xxx&runId=xxx` - Send signal to running workflow
- `POST /api/workflow/query?workflowId=xxx&runId=xxx` - Query workflow state
- `GET /api/workflow/status?workflowId=xxx&runId=xxx` - Get workflow status
- `GET /api/workflow/history?workflowId=xxx&runId=xxx` - Get full event history
- `GET /api/workflows` - List all workflows
- `GET /health` - Health check

## CLI Usage

```bash
# Replay a workflow for debugging
temporal-lite replay --workflow-id <wf-id> --run-id <run-id> --verbose

# Export events to JSON file
temporal-lite export-events --workflow-id <wf-id> --run-id <run-id> --output events.json

# Replay from exported events file
temporal-lite replay --workflow-id <wf-id> --run-id <run-id> --events-file events.json --verbose

# Run database migrations
temporal-lite migrate
```

## Example Workflow

```go
func OrderWorkflow(ctx *engine.WorkflowContext) (interface{}, error) {
    state := &OrderState{Status: "CREATED"}
    
    // Register query handler
    ctx.RegisterQuery("getState", func(...) (...) {
        return json.Marshal(state)
    })
    
    // Version check for backwards compatibility
    v := ctx.Version("order-flow-v2", 1, 2)
    
    // Execute activity with retry policy
    retryPolicy := &model.RetryPolicy{
        InitialInterval:    1 * time.Second,
        BackoffCoefficient: 2,
        MaxAttempts:        5,
    }
    
    fut := ctx.ExecuteActivity("processPayment", input, model.ActivityOptions{
        RetryPolicy: retryPolicy,
    })
    
    result, err := fut.Get(ctx.Context())
    
    // Sleep for 7 days (persisted, not in-memory)
    ctx.Sleep(7 * 24 * time.Hour)
    
    // Await external signal
    signal := ctx.AwaitSignal("updateOrder")
    
    return result, nil
}
```

## Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  API Server     │────▶│  Engine         │────▶│  PostgreSQL     │
│  (port 8132)    │     │  Event Sourcing │     │  Persistence    │
└─────────────────┘     └─────────────────┘     └─────────────────┘
                               ▲
                               │
                        ┌──────┴───────┐
                        │              │
                ┌───────┴──┐    ┌──────┴───────┐
                │ Worker   │    │ Timer Loop   │
                │ Activity │    │ (1s ticker)  │
                │ Executor │    │              │
                └──────────┘    └──────────────┘
```

## Project Structure

```
.
├── cmd/
│   ├── server/         # Server main entry
│   └── cli/            # CLI tool main entry
├── internal/
│   ├── api/            # HTTP API handlers
│   ├── engine/         # Core workflow engine + event sourcing
│   ├── worker/         # Activity worker with retry logic
│   ├── store/          # PostgreSQL data access layer
│   ├── model/          # Data models and types
│   └── common/         # Shared utilities
├── examples/           # Example workflows and activities
├── migrations/         # Database migration scripts
├── Dockerfile
└── docker-compose.yml
```
