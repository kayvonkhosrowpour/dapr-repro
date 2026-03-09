package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
)

const (
	processingDuration    = 20 * time.Second
	serverShutdownTimeout = 60 * time.Second
	consumerTag           = "dapr-repro"
)

type App struct {
	healthy           atomic.Bool
	logger            *zap.Logger
	publishCh         *amqp.Channel
	completionExchange string
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

	rabbitURL := getEnv("RABBITMQ_URL", "amqp://rabbit:rabbit@rabbitmq.rabbit.svc.cluster.local:5672")
	incomingExchange := getEnv("INCOMING_EXCHANGE", "incoming-events")
	incomingQueue := getEnv("INCOMING_QUEUE", "dapr-repro-incoming-events")
	completionExchange := getEnv("COMPLETION_EXCHANGE", "completed-events")

	logger.Info("application starting",
		zap.String("incoming_exchange", incomingExchange),
		zap.String("incoming_queue", incomingQueue),
		zap.String("completion_exchange", completionExchange),
	)

	// --- Connect to RabbitMQ ---
	conn, err := amqp.Dial(rabbitURL)
	if err != nil {
		logger.Fatal("failed to connect to RabbitMQ", zap.Error(err))
	}
	defer conn.Close()

	consumeCh, err := conn.Channel()
	if err != nil {
		logger.Fatal("failed to open consume channel", zap.Error(err))
	}
	defer consumeCh.Close()

	if err := consumeCh.Qos(1, 0, false); err != nil {
		logger.Fatal("failed to set QoS", zap.Error(err))
	}

	publishCh, err := conn.Channel()
	if err != nil {
		logger.Fatal("failed to open publish channel", zap.Error(err))
	}
	defer publishCh.Close()

	// --- Declare infrastructure (idempotent) ---
	dlxExchange := fmt.Sprintf("dlx-%s", incomingQueue)
	dlqQueue := fmt.Sprintf("dlq-%s", incomingQueue)

	if err := consumeCh.ExchangeDeclare(incomingExchange, "topic", true, false, false, false, nil); err != nil {
		logger.Fatal("failed to declare incoming exchange", zap.Error(err), zap.String("exchange", incomingExchange))
	}

	if err := consumeCh.ExchangeDeclare(completionExchange, "topic", true, false, false, false, nil); err != nil {
		logger.Fatal("failed to declare completion exchange", zap.Error(err), zap.String("exchange", completionExchange))
	}

	if err := consumeCh.ExchangeDeclare(dlxExchange, "fanout", true, false, false, false, nil); err != nil {
		logger.Fatal("failed to declare DLX exchange", zap.Error(err), zap.String("exchange", dlxExchange))
	}

	_, err = consumeCh.QueueDeclare(dlqQueue, true, false, false, false, amqp.Table{
		"x-queue-mode": "lazy",
	})
	if err != nil {
		logger.Fatal("failed to declare DLQ queue", zap.Error(err), zap.String("queue", dlqQueue))
	}

	if err := consumeCh.QueueBind(dlqQueue, "#", dlxExchange, false, nil); err != nil {
		logger.Fatal("failed to bind DLQ queue", zap.Error(err))
	}

	_, err = consumeCh.QueueDeclare(incomingQueue, true, false, false, false, amqp.Table{
		"x-dead-letter-exchange": dlxExchange,
	})
	if err != nil {
		logger.Fatal("failed to declare incoming queue", zap.Error(err), zap.String("queue", incomingQueue))
	}

	if err := consumeCh.QueueBind(incomingQueue, "#", incomingExchange, false, nil); err != nil {
		logger.Fatal("failed to bind incoming queue", zap.Error(err))
	}

	logger.Info("RabbitMQ infrastructure declared",
		zap.String("exchange", incomingExchange),
		zap.String("queue", incomingQueue),
		zap.String("completion_exchange", completionExchange),
		zap.String("dlx_exchange", dlxExchange),
		zap.String("dlq_queue", dlqQueue),
	)

	app := &App{
		logger:             logger,
		publishCh:          publishCh,
		completionExchange: completionExchange,
	}
	app.healthy.Store(true)

	// --- Start consuming ---
	deliveries, err := consumeCh.Consume(incomingQueue, consumerTag, false, false, false, false, nil)
	if err != nil {
		logger.Fatal("failed to start consuming", zap.Error(err))
	}

	var inflight sync.WaitGroup

	go func() {
		for d := range deliveries {
			inflight.Add(1)
			app.processDelivery(d, &inflight)
		}
		logger.Info("delivery channel closed — consumer stopped")
	}()

	// --- HTTP server for /healthz ---
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", app.healthHandler)

	server := &http.Server{Addr: ":8080", Handler: mux}
	go func() {
		logger.Info("HTTP server listening (healthz only)", zap.String("addr", server.Addr))
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatal("HTTP server error", zap.Error(err))
		}
	}()

	// --- Wait for shutdown signal ---
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	sig := <-quit

	logger.Info("shutdown signal received", zap.String("signal", sig.String()))

	// Step 1: Cancel AMQP consumer. RabbitMQ stops delivering immediately.
	// Queued messages stay visible for other consumers.
	logger.Info("canceling AMQP consumer — no more deliveries after this")
	if err := consumeCh.Cancel(consumerTag, false); err != nil {
		logger.Error("failed to cancel consumer", zap.Error(err))
	}

	// Step 2: Mark unhealthy for K8s readiness probes.
	app.healthy.Store(false)
	logger.Info("application marked unhealthy")

	// Step 3: Wait for in-flight message to finish + ACK.
	logger.Info("waiting for in-flight message to drain")
	inflight.Wait()
	logger.Info("in-flight messages drained")

	// Step 4: Close AMQP.
	publishCh.Close()
	consumeCh.Close()
	conn.Close()
	logger.Info("AMQP connection closed")

	// Step 5: Shutdown HTTP server.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), serverShutdownTimeout)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error("HTTP server shutdown error", zap.Error(err))
	}

	logger.Info("all resources released — application exiting")
}

func (a *App) processDelivery(d amqp.Delivery, wg *sync.WaitGroup) {
	defer wg.Done()

	var payload map[string]any
	eventID := "unknown"
	if err := json.Unmarshal(d.Body, &payload); err == nil {
		if id, ok := payload["id"]; ok {
			eventID = fmt.Sprintf("%v", id)
		}
	}

	a.logger.Info("event received — beginning processing",
		zap.String("event_id", eventID),
		zap.String("body", string(d.Body)),
		zap.Uint64("delivery_tag", d.DeliveryTag),
	)

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	elapsed := 0
	for elapsed < int(processingDuration.Seconds()) {
		<-ticker.C
		elapsed++
		a.logger.Info("event processing tick",
			zap.String("event_id", eventID),
			zap.Int("second", elapsed),
			zap.Int("total", int(processingDuration.Seconds())),
		)
	}

	a.logger.Info("event processing complete — publishing completion",
		zap.String("event_id", eventID),
		zap.String("exchange", a.completionExchange),
	)

	if err := a.publishCompletion(eventID); err != nil {
		a.logger.Error("publish failed — NACKing message (will requeue)",
			zap.Error(err),
			zap.String("event_id", eventID),
		)
		if nackErr := d.Nack(false, true); nackErr != nil {
			a.logger.Error("NACK failed", zap.Error(nackErr), zap.String("event_id", eventID))
		}
		return
	}

	a.logger.Info("completion published — ACKing message",
		zap.String("event_id", eventID),
	)
	if err := d.Ack(false); err != nil {
		a.logger.Error("ACK failed", zap.Error(err), zap.String("event_id", eventID))
	}
}

func (a *App) healthHandler(w http.ResponseWriter, r *http.Request) {
	if !a.healthy.Load() {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		fmt.Fprint(w, `{"status":"unhealthy"}`)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, `{"status":"healthy"}`)
}

func (a *App) publishCompletion(eventID string) error {
	payload := map[string]any{
		"completedAt": time.Now().UTC().Format(time.RFC3339),
		"eventId":     eventID,
		"message":     "event processing completed successfully",
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	return a.publishCh.PublishWithContext(ctx, a.completionExchange, "completion", false, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Body:         body,
	})
}
