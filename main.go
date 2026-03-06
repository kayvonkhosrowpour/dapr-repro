package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"go.uber.org/zap"
)

const (
	processingDuration   = 20 * time.Second
	healthProbeWait      = 10 * time.Second
	serverShutdownTimeout = 60 * time.Second
)

type App struct {
	healthy         atomic.Bool
	logger          *zap.Logger
	pubsubName      string
	completionTopic string
	daprBaseURL     string
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func main() {
	logger, err := zap.NewProduction()
	if err != nil {
		panic(fmt.Sprintf("failed to initialize logger: %v", err))
	}
	defer logger.Sync() //nolint:errcheck

	app := &App{
		logger:          logger,
		pubsubName:      getEnv("PUBSUB_NAME", "pubsub"),
		completionTopic: getEnv("COMPLETION_TOPIC", "completed-events"),
		daprBaseURL:     fmt.Sprintf("http://localhost:%s", getEnv("DAPR_HTTP_PORT", "3500")),
	}
	app.healthy.Store(true)

	logger.Info("application starting",
		zap.String("pubsub_name", app.pubsubName),
		zap.String("completion_topic", app.completionTopic),
		zap.String("dapr_base_url", app.daprBaseURL),
	)

	r := chi.NewRouter()
	r.Use(middleware.Recoverer)

	r.Get("/healthz", app.healthHandler)
	r.Post("/events/incoming", app.handleIncomingEvent)

	server := &http.Server{
		Addr:    ":8080",
		Handler: r,
	}

	serverErr := make(chan error, 1)
	go func() {
		logger.Info("HTTP server listening", zap.String("addr", server.Addr))
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			serverErr <- err
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	select {
	case err := <-serverErr:
		logger.Fatal("server error", zap.Error(err))
	case sig := <-quit:
		logger.Info("shutdown signal received",
			zap.String("signal", sig.String()),
		)
	}

	// Step 1: Mark unhealthy so Dapr's health probes detect the state change
	// and stop routing new pub/sub events to this app.
	app.healthy.Store(false)
	logger.Info("application marked unhealthy — Dapr will stop routing new events")

	// Step 2: Wait for Dapr's health probes to pick up the unhealthy state.
	// With probe interval=3s and threshold=2, allow 2 full probe cycles (~10s).
	logger.Info("waiting for Dapr health probes to detect unhealthy state",
		zap.Duration("wait", healthProbeWait),
	)
	time.Sleep(healthProbeWait)

	// Step 3: Stop accepting new HTTP connections; wait for in-flight handlers to finish.
	// server.Shutdown blocks until all active connections (i.e. the in-progress event
	// handler) have returned.
	logger.Info("draining in-flight event handlers",
		zap.Duration("timeout", serverShutdownTimeout),
	)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), serverShutdownTimeout)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error("HTTP server shutdown error", zap.Error(err))
	}

	logger.Info("all handlers drained — application exiting")
}

func (a *App) healthHandler(w http.ResponseWriter, r *http.Request) {
	if !a.healthy.Load() {
		a.logger.Debug("health probe responded unhealthy")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		fmt.Fprint(w, `{"status":"unhealthy"}`)
		return
	}
	a.logger.Debug("health probe responded healthy")
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, `{"status":"healthy"}`)
}

func (a *App) handleIncomingEvent(w http.ResponseWriter, r *http.Request) {
	a.logger.Info("event received — beginning processing",
		zap.String("method", r.Method),
		zap.String("path", r.URL.Path),
		zap.String("remote_addr", r.RemoteAddr),
	)

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	elapsed := 0
	for elapsed < int(processingDuration.Seconds()) {
		<-ticker.C
		elapsed++
		a.logger.Info("event processing tick",
			zap.Int("second", elapsed),
			zap.Int("total", int(processingDuration.Seconds())),
		)
	}

	a.logger.Info("event processing complete — publishing completion event",
		zap.String("pubsub", a.pubsubName),
		zap.String("topic", a.completionTopic),
	)

	if err := a.publishCompletion(r.Context()); err != nil {
		a.logger.Error("publish failed — returning RETRY",
			zap.Error(err),
			zap.String("pubsub", a.pubsubName),
			zap.String("topic", a.completionTopic),
		)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{"status": "RETRY"}) //nolint:errcheck
		return
	}

	a.logger.Info("completion event published successfully",
		zap.String("pubsub", a.pubsubName),
		zap.String("topic", a.completionTopic),
	)

	a.logger.Info("handler returning SUCCESS")
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "SUCCESS"}) //nolint:errcheck
}

func (a *App) publishCompletion(ctx context.Context) error {
	payload := map[string]any{
		"completedAt": time.Now().UTC().Format(time.RFC3339),
		"message":     "event processing completed successfully",
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}

	url := fmt.Sprintf("%s/v1.0/publish/%s/%s", a.daprBaseURL, a.pubsubName, a.completionTopic)

	a.logger.Debug("publishing to Dapr HTTP API",
		zap.String("url", url),
		zap.ByteString("body", body),
	)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("http post: %w", err)
	}
	defer resp.Body.Close()

	a.logger.Debug("Dapr publish response",
		zap.Int("status_code", resp.StatusCode),
	)

	// Dapr returns 204 No Content on successful publish
	if resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("unexpected Dapr response status: %d", resp.StatusCode)
	}

	return nil
}
