package health_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"go.uber.org/zap"

	"github.com/zhavkk/Diploma/pkg/models"
	"github.com/zhavkk/Diploma/services/node-agent/internal/health"
)

// ─────────────────────────────────────────
// Mock StatusProvider
// ─────────────────────────────────────────

type mockStatusProvider struct {
	status *models.NodeStatus
}

func (m *mockStatusProvider) Latest() *models.NodeStatus { return m.status }

// ─────────────────────────────────────────
// /health/primary
// ─────────────────────────────────────────

func TestHealthServer_Primary_Returns200WhenPrimary(t *testing.T) {
	srv := health.NewServer(health.Config{Addr: ":0"}, &mockStatusProvider{
		status: &models.NodeStatus{IsInRecovery: false, PostgresRunning: true},
	}, zap.NewNop())

	w := httptest.NewRecorder()
	srv.HandlePrimary(w, httptest.NewRequest(http.MethodGet, "/health/primary", nil))

	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", w.Code)
	}
	if w.Body.String() != "primary" {
		t.Errorf("body = %q, want %q", w.Body.String(), "primary")
	}
}

func TestHealthServer_Primary_Returns503WhenReplica(t *testing.T) {
	srv := health.NewServer(health.Config{Addr: ":0"}, &mockStatusProvider{
		status: &models.NodeStatus{IsInRecovery: true},
	}, zap.NewNop())

	w := httptest.NewRecorder()
	srv.HandlePrimary(w, httptest.NewRequest(http.MethodGet, "/health/primary", nil))

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", w.Code)
	}
}

func TestHealthServer_Primary_Returns503WhenNilStatus(t *testing.T) {
	srv := health.NewServer(health.Config{Addr: ":0"}, &mockStatusProvider{status: nil}, zap.NewNop())

	w := httptest.NewRecorder()
	srv.HandlePrimary(w, httptest.NewRequest(http.MethodGet, "/health/primary", nil))

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", w.Code)
	}
}

func TestHealthServer_Primary_Returns503WhenPostgresNotRunning(t *testing.T) {
	// PostgresRunning=false, IsInRecovery=false — crashed primary should return 503.
	srv := health.NewServer(health.Config{Addr: ":0"}, &mockStatusProvider{
		status: &models.NodeStatus{IsInRecovery: false, PostgresRunning: false},
	}, zap.NewNop())

	w := httptest.NewRecorder()
	srv.HandlePrimary(w, httptest.NewRequest(http.MethodGet, "/health/primary", nil))

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503 when PostgresRunning=false", w.Code)
	}
}

func TestHealthServer_Replica_Returns503WhenPostgresNotRunning(t *testing.T) {
	// PostgresRunning=false, IsInRecovery=true — crashed replica should return 503.
	srv := health.NewServer(health.Config{Addr: ":0"}, &mockStatusProvider{
		status: &models.NodeStatus{IsInRecovery: true, PostgresRunning: false},
	}, zap.NewNop())

	w := httptest.NewRecorder()
	srv.HandleReplica(w, httptest.NewRequest(http.MethodGet, "/health/replica", nil))

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503 when PostgresRunning=false", w.Code)
	}
}

// ─────────────────────────────────────────
// /health/replica
// ─────────────────────────────────────────

func TestHealthServer_Replica_Returns200WhenReplica(t *testing.T) {
	srv := health.NewServer(health.Config{Addr: ":0"}, &mockStatusProvider{
		status: &models.NodeStatus{IsInRecovery: true, PostgresRunning: true},
	}, zap.NewNop())

	w := httptest.NewRecorder()
	srv.HandleReplica(w, httptest.NewRequest(http.MethodGet, "/health/replica", nil))

	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", w.Code)
	}
	if w.Body.String() != "replica" {
		t.Errorf("body = %q, want %q", w.Body.String(), "replica")
	}
}

func TestHealthServer_Replica_Returns503WhenPrimary(t *testing.T) {
	srv := health.NewServer(health.Config{Addr: ":0"}, &mockStatusProvider{
		status: &models.NodeStatus{IsInRecovery: false},
	}, zap.NewNop())

	w := httptest.NewRecorder()
	srv.HandleReplica(w, httptest.NewRequest(http.MethodGet, "/health/replica", nil))

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", w.Code)
	}
}

// ─────────────────────────────────────────
// /health/alive
// ─────────────────────────────────────────

func TestHealthServer_Alive_Returns200WhenRunning(t *testing.T) {
	srv := health.NewServer(health.Config{Addr: ":0"}, &mockStatusProvider{
		status: &models.NodeStatus{PostgresRunning: true},
	}, zap.NewNop())

	w := httptest.NewRecorder()
	srv.HandleAlive(w, httptest.NewRequest(http.MethodGet, "/health/alive", nil))

	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", w.Code)
	}
}

func TestHealthServer_Alive_Returns503WhenNotRunning(t *testing.T) {
	srv := health.NewServer(health.Config{Addr: ":0"}, &mockStatusProvider{
		status: &models.NodeStatus{PostgresRunning: false},
	}, zap.NewNop())

	w := httptest.NewRecorder()
	srv.HandleAlive(w, httptest.NewRequest(http.MethodGet, "/health/alive", nil))

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", w.Code)
	}
}
