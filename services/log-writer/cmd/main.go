package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	_ "github.com/lib/pq"
)

type config struct {
	DSN          string
	ServiceName  string
	InstanceName string
	Interval     time.Duration
	HTTPAddr     string
}

func main() {
	cfg := loadConfig()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	db, err := sql.Open("postgres", cfg.DSN)
	if err != nil {
		log.Fatalf("open db: %v", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(2)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(30 * time.Second)

	if cfg.HTTPAddr != "" {
		go runHealthServer(ctx, cfg.HTTPAddr)
	}

	log.Printf("app-log-writer started service=%s instance=%s interval=%s", cfg.ServiceName, cfg.InstanceName, cfg.Interval)

	waitUntilReady(ctx, db)
	writeLoop(ctx, db, cfg)
}

func loadConfig() config {
	return config{
		DSN:          envOr("DATABASE_URL", "postgres://postgres:postgres@localhost:5000/postgres?sslmode=disable&connect_timeout=1"),
		ServiceName:  envOr("SERVICE_NAME", "demo-log-service"),
		InstanceName: envOr("INSTANCE_NAME", hostname()),
		Interval:     envDuration("WRITE_INTERVAL", time.Second),
		HTTPAddr:     envOr("HTTP_ADDR", ":8090"),
	}
}

func waitUntilReady(ctx context.Context, db *sql.DB) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		if err := ensureSchema(ctx, db); err == nil {
			log.Print("database schema is ready")
			return
		} else {
			log.Printf("database is not ready yet: %v", err)
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func ensureSchema(parent context.Context, db *sql.DB) error {
	ctx, cancel := context.WithTimeout(parent, 2*time.Second)
	defer cancel()

	_, err := db.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS app_logs (
	id bigserial PRIMARY KEY,
	created_at timestamptz NOT NULL DEFAULT now(),
	service text NOT NULL,
	instance text NOT NULL,
	level text NOT NULL,
	message text NOT NULL,
	sequence bigint NOT NULL
);`)
	return err
}

func writeLoop(ctx context.Context, db *sql.DB, cfg config) {
	ticker := time.NewTicker(cfg.Interval)
	defer ticker.Stop()

	var seq int64
	for {
		seq++
		writeLog(ctx, db, cfg, seq)

		select {
		case <-ctx.Done():
			log.Print("app-log-writer stopped")
			return
		case <-ticker.C:
		}
	}
}

func writeLog(parent context.Context, db *sql.DB, cfg config, seq int64) {
	ctx, cancel := context.WithTimeout(parent, 2*time.Second)
	defer cancel()

	level := randomLevel()
	message := fmt.Sprintf("application event #%d", seq)
	_, err := db.ExecContext(ctx, `
INSERT INTO app_logs (service, instance, level, message, sequence)
VALUES ($1, $2, $3, $4, $5);`,
		cfg.ServiceName, cfg.InstanceName, level, message, seq)
	if err != nil {
		log.Printf("write failed seq=%d error=%v", seq, err)
		return
	}
	log.Printf("insert ok seq=%d level=%s message=%q", seq, level, message)
}

func randomLevel() string {
	switch rand.Intn(4) {
	case 0:
		return "debug"
	case 1:
		return "info"
	case 2:
		return "warn"
	default:
		return "error"
	}
}

func runHealthServer(ctx context.Context, addr string) {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})

	srv := &http.Server{Addr: addr, Handler: mux}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Printf("health server error: %v", err)
	}
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func envDuration(key string, fallback time.Duration) time.Duration {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback
	}
	if d, err := time.ParseDuration(raw); err == nil && d > 0 {
		return d
	}
	if seconds, err := strconv.Atoi(raw); err == nil && seconds > 0 {
		return time.Duration(seconds) * time.Second
	}
	return fallback
}

func hostname() string {
	name, err := os.Hostname()
	if err != nil || name == "" {
		return "unknown"
	}
	return name
}
