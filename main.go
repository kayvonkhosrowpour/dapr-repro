package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

const processingDuration = 20 * time.Second

type App struct {
	healthy atomic.Bool
	logger  *zap.Logger
}

func main() {
	logger, err := zap.NewProduction()
	if err != nil {
		panic(fmt.Sprintf("failed to initialize logger: %v", err))
	}
	defer logger.Sync() //nolint:errcheck

	app := &App{logger: logger}
	app.healthy.Store(true)

	mux := http.NewServeMux()
	mux.HandleFunc("/dapr/subscribe", app.subscribeHandler)
	mux.HandleFunc("/events", app.eventsHandler)
	mux.HandleFunc("/healthz", app.healthHandler)

	server := &http.Server{Addr: ":8080", Handler: mux}

	logger.Info("starting HTTP server", zap.String("addr", server.Addr))
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.Fatal("HTTP server error", zap.Error(err))
	}
}

func (a *App) subscribeHandler(w http.ResponseWriter, r *http.Request) {
	subscriptions := []map[string]string{
		{
			"pubsubname": "pubsub",
			"topic":      "incoming-events",
			"route":      "/events",
		},
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(subscriptions) //nolint:errcheck
}

func (a *App) eventsHandler(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		a.logger.Error("failed to read request body", zap.Error(err))
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	defer r.Body.Close()

	var event map[string]any
	if err := json.Unmarshal(body, &event); err != nil {
		a.logger.Error("failed to unmarshal event", zap.Error(err))
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	data, _ := event["data"].(map[string]any)
	eventID := "unknown"
	if data != nil {
		if id, ok := data["id"]; ok {
			eventID = fmt.Sprintf("%v", id)
		}
	}

	a.logger.Info("event received — beginning processing",
		zap.String("event_id", eventID),
	)

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	elapsed := 0
	for elapsed < int(processingDuration.Seconds()) {
		<-ticker.C
		elapsed++
		a.logger.Info("processing tick",
			zap.String("event_id", eventID),
			zap.Int("second", elapsed),
			zap.Int("total", int(processingDuration.Seconds())),
		)
	}

	a.logger.Info("event processing complete", zap.String("event_id", eventID))

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, `{"status":"SUCCESS"}`)
}

func (a *App) healthHandler(w http.ResponseWriter, r *http.Request) {
	if !a.healthy.Load() {
		w.WriteHeader(http.StatusServiceUnavailable)
		fmt.Fprint(w, `{"status":"unhealthy"}`)
		return
	}
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, `{"status":"healthy"}`)
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
