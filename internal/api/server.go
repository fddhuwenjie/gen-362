package api

import (
	"context"
	"encoding/json"
	"net/http"
	"temporal-lite/internal/engine"
	"temporal-lite/internal/model"
	"temporal-lite/internal/store"
	"time"
)

type Server struct {
	engine *engine.Engine
	store  store.Store
	server *http.Server
}

type StartWorkflowRequest struct {
	WorkflowType string                 `json:"workflowType"`
	Input        map[string]interface{} `json:"input"`
	WorkflowID   string                 `json:"workflowId,omitempty"`
	Version      string                 `json:"version,omitempty"`
}

type SignalRequest struct {
	SignalName string                 `json:"signalName"`
	Input      map[string]interface{} `json:"input"`
	Version    string                 `json:"version,omitempty"`
}

type QueryRequest struct {
	QueryName string                 `json:"queryName"`
	Input     map[string]interface{} `json:"input"`
}

type Response struct {
	Success bool        `json:"success"`
	Data    interface{} `json:"data,omitempty"`
	Error   string      `json:"error,omitempty"`
}

func NewServer(e *engine.Engine, s store.Store, addr string) *Server {
	mux := http.NewServeMux()
	server := &Server{
		engine: e,
		store:  s,
		server: &http.Server{
			Addr:    addr,
			Handler: mux,
		},
	}
	server.registerRoutes(mux)
	return server
}

func (s *Server) registerRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/workflow/start", s.handleStartWorkflow)
	mux.HandleFunc("/api/workflow/signal", s.handleSendSignal)
	mux.HandleFunc("/api/workflow/query", s.handleQuery)
	mux.HandleFunc("/api/workflow/status", s.handleGetStatus)
	mux.HandleFunc("/api/workflow/history", s.handleGetHistory)
	mux.HandleFunc("/api/workflows", s.handleListWorkflows)
	mux.HandleFunc("/health", s.handleHealth)
}

func (s *Server) Start() error {
	return s.server.ListenAndServe()
}

func (s *Server) Stop(ctx context.Context) error {
	return s.server.Shutdown(ctx)
}

func (s *Server) handleStartWorkflow(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req StartWorkflowRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	ctx := r.Context()
	we, err := s.engine.StartWorkflow(ctx, req.WorkflowType, req.Input, req.WorkflowID, req.Version)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, Response{
		Success: true,
		Data:    we,
	})
}

func (s *Server) handleSendSignal(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	workflowID := r.URL.Query().Get("workflowId")
	runID := r.URL.Query().Get("runId")
	if workflowID == "" || runID == "" {
		writeError(w, http.StatusBadRequest, "workflowId and runId are required")
		return
	}

	var req SignalRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	ctx := r.Context()
	err := s.engine.SendSignal(ctx, workflowID, runID, req.SignalName, req.Input, req.Version)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, Response{
		Success: true,
		Data: map[string]interface{}{
			"message": "signal sent",
		},
	})
}

func (s *Server) handleQuery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	workflowID := r.URL.Query().Get("workflowId")
	runID := r.URL.Query().Get("runId")
	if workflowID == "" || runID == "" {
		writeError(w, http.StatusBadRequest, "workflowId and runId are required")
		return
	}

	var req QueryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	ctx := r.Context()
	inputJSON, _ := json.Marshal(req.Input)
	result, err := s.engine.ExecuteQuery(ctx, workflowID, runID, req.QueryName, inputJSON)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, Response{
		Success: true,
		Data:    json.RawMessage(result),
	})
}

func (s *Server) handleGetStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	workflowID := r.URL.Query().Get("workflowId")
	runID := r.URL.Query().Get("runId")
	if workflowID == "" || runID == "" {
		writeError(w, http.StatusBadRequest, "workflowId and runId are required")
		return
	}

	ctx := r.Context()
	we, err := s.store.GetWorkflowExecution(ctx, workflowID, runID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, Response{
		Success: true,
		Data:    we,
	})
}

func (s *Server) handleGetHistory(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	workflowID := r.URL.Query().Get("workflowId")
	runID := r.URL.Query().Get("runId")
	if workflowID == "" || runID == "" {
		writeError(w, http.StatusBadRequest, "workflowId and runId are required")
		return
	}

	ctx := r.Context()
	events, err := s.store.GetAllEvents(ctx, workflowID, runID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, Response{
		Success: true,
		Data:    events,
	})
}

func (s *Server) handleListWorkflows(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	status := r.URL.Query().Get("status")
	ctx := r.Context()
	workflows, err := s.store.ListWorkflowExecutions(ctx, model.WorkflowStatus(status), 100)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, Response{
		Success: true,
		Data:    workflows,
	})
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, Response{
		Success: true,
		Data: map[string]interface{}{
			"status":    "ok",
			"timestamp": time.Now().UTC(),
		},
	})
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, err string) {
	writeJSON(w, status, Response{
		Success: false,
		Error:   err,
	})
}
