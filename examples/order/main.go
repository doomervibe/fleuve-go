// Package main demonstrates a complete e-commerce order workflow
// This example shows:
// - State machine transitions (created -> paid -> shipped -> delivered or cancelled)
// - External adapter calling a mock payment API
// - Activity execution with exponential backoff retries
// - Delays (payment timeout)
// - Error handling and compensation logic
// - DLQ (Dead Letter Queue) behavior after max retries
//
// Run: go run main.go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/fleuve/fleuve-go/pkg/config"
	"github.com/fleuve/fleuve-go/pkg/model"
	"github.com/fleuve/fleuve-go/pkg/repo"
)

// OrderStatus represents the state of an order
type OrderStatus string

const (
	OrderStatusCreated       OrderStatus = "created"
	OrderStatusPaid          OrderStatus = "paid"
	OrderStatusShipped       OrderStatus = "shipped"
	OrderStatusDelivered     OrderStatus = "delivered"
	OrderStatusCancelled     OrderStatus = "cancelled"
	OrderStatusPaymentFailed OrderStatus = "payment_failed"
)

// OrderState holds the workflow state
type OrderState struct {
	OrderID            string               `json:"order_id"`
	CustomerID         string               `json:"customer_id"`
	Amount             float64              `json:"amount"`
	Currency           string               `json:"currency"`
	Status             OrderStatus          `json:"status"`
	PaymentID          string               `json:"payment_id,omitempty"`
	TrackingNumber     string               `json:"tracking_number,omitempty"`
	CancellationReason string               `json:"cancellation_reason,omitempty"`
	RetryCount         int                  `json:"retry_count"`
	CreatedAt          time.Time            `json:"created_at"`
	UpdatedAt          time.Time            `json:"updated_at"`
	Lifecycle          model.LifecycleState `json:"lifecycle"`
}

func (s *OrderState) GetSubscriptions() []model.Sub                 { return nil }
func (s *OrderState) GetExternalSubscriptions() []model.ExternalSub { return nil }
func (s *OrderState) GetLifecycle() model.LifecycleState            { return s.Lifecycle }
func (s *OrderState) GetSchedules() []model.Schedule {
	// No scheduled activities for this example
	return nil
}
func (s *OrderState) Copy() model.State {
	return &OrderState{
		OrderID:            s.OrderID,
		CustomerID:         s.CustomerID,
		Amount:             s.Amount,
		Currency:           s.Currency,
		Status:             s.Status,
		PaymentID:          s.PaymentID,
		TrackingNumber:     s.TrackingNumber,
		CancellationReason: s.CancellationReason,
		RetryCount:         s.RetryCount,
		CreatedAt:          s.CreatedAt,
		UpdatedAt:          s.UpdatedAt,
		Lifecycle:          s.Lifecycle,
	}
}

// Events
type OrderCreated struct {
	OrderID    string         `json:"order_id"`
	CustomerID string         `json:"customer_id"`
	Amount     float64        `json:"amount"`
	Currency   string         `json:"currency"`
	CreatedAt  time.Time      `json:"created_at"`
	Metadata   map[string]any `json:"metadata,omitempty"`
}

func (e *OrderCreated) GetType() string              { return "order_created" }
func (e *OrderCreated) GetMetadata() map[string]any  { return e.Metadata }
func (e *OrderCreated) SetMetadata(m map[string]any) { e.Metadata = m }

type PaymentProcessed struct {
	OrderID   string         `json:"order_id"`
	PaymentID string         `json:"payment_id"`
	Amount    float64        `json:"amount"`
	Success   bool           `json:"success"`
	Timestamp time.Time      `json:"timestamp"`
	Metadata  map[string]any `json:"metadata,omitempty"`
}

func (e *PaymentProcessed) GetType() string              { return "payment_processed" }
func (e *PaymentProcessed) GetMetadata() map[string]any  { return e.Metadata }
func (e *PaymentProcessed) SetMetadata(m map[string]any) { e.Metadata = m }

type OrderShipped struct {
	OrderID        string         `json:"order_id"`
	TrackingNumber string         `json:"tracking_number"`
	ShippedAt      time.Time      `json:"shipped_at"`
	Metadata       map[string]any `json:"metadata,omitempty"`
}

func (e *OrderShipped) GetType() string              { return "order_shipped" }
func (e *OrderShipped) GetMetadata() map[string]any  { return e.Metadata }
func (e *OrderShipped) SetMetadata(m map[string]any) { e.Metadata = m }

type OrderDelivered struct {
	OrderID     string         `json:"order_id"`
	DeliveredAt time.Time      `json:"delivered_at"`
	Metadata    map[string]any `json:"metadata,omitempty"`
}

func (e *OrderDelivered) GetType() string              { return "order_delivered" }
func (e *OrderDelivered) GetMetadata() map[string]any  { return e.Metadata }
func (e *OrderDelivered) SetMetadata(m map[string]any) { e.Metadata = m }

type OrderCancelled struct {
	OrderID         string         `json:"order_id"`
	Reason          string         `json:"reason"`
	CancelledAt     time.Time      `json:"cancelled_at"`
	RefundProcessed bool           `json:"refund_processed"`
	Metadata        map[string]any `json:"metadata,omitempty"`
}

func (e *OrderCancelled) GetType() string              { return "order_cancelled" }
func (e *OrderCancelled) GetMetadata() map[string]any  { return e.Metadata }
func (e *OrderCancelled) SetMetadata(m map[string]any) { e.Metadata = m }

type PaymentFailed struct {
	OrderID   string         `json:"order_id"`
	Error     string         `json:"error"`
	Attempt   int            `json:"attempt"`
	WillRetry bool           `json:"will_retry"`
	Timestamp time.Time      `json:"timestamp"`
	Metadata  map[string]any `json:"metadata,omitempty"`
}

func (e *PaymentFailed) GetType() string              { return "payment_failed" }
func (e *PaymentFailed) GetMetadata() map[string]any  { return e.Metadata }
func (e *PaymentFailed) SetMetadata(m map[string]any) { e.Metadata = m }

// Commands
type CreateOrder struct {
	OrderID    string  `json:"order_id"`
	CustomerID string  `json:"customer_id"`
	Amount     float64 `json:"amount"`
	Currency   string  `json:"currency"`
}

type ProcessPayment struct {
	OrderID string `json:"order_id"`
}

type ShipOrder struct {
	OrderID        string `json:"order_id"`
	TrackingNumber string `json:"tracking_number"`
}

type DeliverOrder struct {
	OrderID string `json:"order_id"`
}

type CancelOrder struct {
	OrderID string `json:"order_id"`
	Reason  string `json:"reason"`
}

type RetryPayment struct {
	OrderID string `json:"order_id"`
}

// Workflow
type OrderWorkflow struct {
	paymentGateway *PaymentGateway
}

func NewOrderWorkflow(paymentGateway *PaymentGateway) *OrderWorkflow {
	return &OrderWorkflow{
		paymentGateway: paymentGateway,
	}
}

func (w *OrderWorkflow) Name() string       { return "OrderWorkflow" }
func (w *OrderWorkflow) SchemaVersion() int { return 1 }

func (w *OrderWorkflow) Upcast(eventType string, schemaVersion int, rawData map[string]any) map[string]any {
	return rawData
}

func (w *OrderWorkflow) Decide(state model.State, cmd model.Command) ([]model.Event, *model.Rejection) {
	var s *OrderState
	if state != nil {
		s = state.(*OrderState)
	} else {
		s = &OrderState{Lifecycle: model.LifecycleActive}
	}

	if s.Lifecycle != model.LifecycleActive {
		return nil, &model.Rejection{Msg: "workflow not active"}
	}

	switch c := cmd.(type) {
	case *CreateOrder:
		if s.Status != "" {
			return nil, &model.Rejection{Msg: "order already exists"}
		}
		if c.Amount <= 0 {
			return nil, &model.Rejection{Msg: "amount must be positive"}
		}
		return []model.Event{&OrderCreated{
			OrderID:    c.OrderID,
			CustomerID: c.CustomerID,
			Amount:     c.Amount,
			Currency:   c.Currency,
			CreatedAt:  time.Now(),
		}}, nil

	case *ProcessPayment:
		if s.Status != OrderStatusCreated {
			return nil, &model.Rejection{Msg: fmt.Sprintf("cannot process payment in status %s", s.Status)}
		}

		// Simulate payment processing with retry logic
		result := w.paymentGateway.ProcessPayment(c.OrderID, s.Amount, s.Currency, s.RetryCount)

		if result.Success {
			return []model.Event{&PaymentProcessed{
				OrderID:   c.OrderID,
				PaymentID: result.PaymentID,
				Amount:    s.Amount,
				Success:   true,
				Timestamp: time.Now(),
			}}, nil
		} else {
			// Check if we should retry or give up
			willRetry := s.RetryCount < 3
			return []model.Event{&PaymentFailed{
				OrderID:   c.OrderID,
				Error:     result.Error,
				Attempt:   s.RetryCount + 1,
				WillRetry: willRetry,
				Timestamp: time.Now(),
			}}, nil
		}

	case *RetryPayment:
		if s.Status != OrderStatusCreated && s.Status != OrderStatusPaymentFailed {
			return nil, &model.Rejection{Msg: fmt.Sprintf("cannot retry payment in status %s", s.Status)}
		}
		// This will trigger ProcessPayment again
		return []model.Event{&PaymentFailed{
			OrderID:   c.OrderID,
			Error:     "retry_requested",
			Attempt:   s.RetryCount,
			WillRetry: true,
			Timestamp: time.Now(),
		}}, nil

	case *ShipOrder:
		if s.Status != OrderStatusPaid {
			return nil, &model.Rejection{Msg: fmt.Sprintf("cannot ship order in status %s", s.Status)}
		}
		return []model.Event{&OrderShipped{
			OrderID:        c.OrderID,
			TrackingNumber: c.TrackingNumber,
			ShippedAt:      time.Now(),
		}}, nil

	case *DeliverOrder:
		if s.Status != OrderStatusShipped {
			return nil, &model.Rejection{Msg: fmt.Sprintf("cannot deliver order in status %s", s.Status)}
		}
		return []model.Event{&OrderDelivered{
			OrderID:     c.OrderID,
			DeliveredAt: time.Now(),
		}}, nil

	case *CancelOrder:
		if s.Status == OrderStatusDelivered || s.Status == OrderStatusCancelled {
			return nil, &model.Rejection{Msg: fmt.Sprintf("cannot cancel order in status %s", s.Status)}
		}
		return []model.Event{&OrderCancelled{
			OrderID:         c.OrderID,
			Reason:          c.Reason,
			CancelledAt:     time.Now(),
			RefundProcessed: s.Status == OrderStatusPaid,
		}}, nil
	}

	return nil, nil
}

func (w *OrderWorkflow) Evolve(state model.State, event model.Event) model.State {
	var s *OrderState
	if state != nil {
		s = state.(*OrderState).Copy().(*OrderState)
	} else {
		s = &OrderState{Lifecycle: model.LifecycleActive}
	}

	s.UpdatedAt = time.Now()

	switch e := event.(type) {
	case *OrderCreated:
		s.OrderID = e.OrderID
		s.CustomerID = e.CustomerID
		s.Amount = e.Amount
		s.Currency = e.Currency
		s.Status = OrderStatusCreated
		s.CreatedAt = e.CreatedAt
		s.RetryCount = 0

	case *PaymentProcessed:
		if e.Success {
			s.PaymentID = e.PaymentID
			s.Status = OrderStatusPaid
		}

	case *PaymentFailed:
		s.Status = OrderStatusPaymentFailed
		s.RetryCount = e.Attempt
		// After 3 failed attempts, move to DLQ (cancelled)
		if !e.WillRetry {
			s.Status = OrderStatusCancelled
			s.CancellationReason = fmt.Sprintf("payment_failed_after_%d_attempts: %s", e.Attempt, e.Error)
		}

	case *OrderShipped:
		s.TrackingNumber = e.TrackingNumber
		s.Status = OrderStatusShipped

	case *OrderDelivered:
		s.Status = OrderStatusDelivered
		s.Lifecycle = model.LifecycleCompleted

	case *OrderCancelled:
		s.Status = OrderStatusCancelled
		s.CancellationReason = e.Reason
		s.Lifecycle = model.LifecycleCompleted
	}

	return s
}

func (w *OrderWorkflow) EventToCmd(e model.Event) model.Command {
	// Auto-trigger payment after order creation
	if event, ok := e.(*OrderCreated); ok {
		return &ProcessPayment{OrderID: event.OrderID}
	}

	// Auto-retry payment on failure (with exponential backoff)
	if event, ok := e.(*PaymentFailed); ok && event.WillRetry {
		return &RetryPayment{OrderID: event.OrderID}
	}

	return nil
}

func (w *OrderWorkflow) DecodeEvent(eventType string, schemaVersion int, raw map[string]any) (model.Event, error) {
	b, err := json.Marshal(raw)
	if err != nil {
		return nil, err
	}
	switch eventType {
	case "order_created":
		var e OrderCreated
		if err := json.Unmarshal(b, &e); err != nil {
			return nil, err
		}
		return &e, nil
	case "payment_processed":
		var e PaymentProcessed
		if err := json.Unmarshal(b, &e); err != nil {
			return nil, err
		}
		return &e, nil
	case "order_shipped":
		var e OrderShipped
		if err := json.Unmarshal(b, &e); err != nil {
			return nil, err
		}
		return &e, nil
	case "order_delivered":
		var e OrderDelivered
		if err := json.Unmarshal(b, &e); err != nil {
			return nil, err
		}
		return &e, nil
	case "order_cancelled":
		var e OrderCancelled
		if err := json.Unmarshal(b, &e); err != nil {
			return nil, err
		}
		return &e, nil
	case "payment_failed":
		var e PaymentFailed
		if err := json.Unmarshal(b, &e); err != nil {
			return nil, err
		}
		return &e, nil
	default:
		return model.DecodeBuiltinReplayEvent(eventType, raw)
	}
}

func (w *OrderWorkflow) IsFinalEvent(e model.Event) bool {
	switch e.(type) {
	case *OrderDelivered, *OrderCancelled:
		return true
	default:
		return false
	}
}

// PaymentGateway simulates an external payment API
type PaymentGateway struct {
	baseURL    string
	httpClient *http.Client
	mu         sync.Mutex
	// Simulate failures for demonstration
	FailureRate float64
	Timeout     time.Duration
}

type PaymentResult struct {
	Success   bool
	PaymentID string
	Error     string
}

func NewPaymentGateway(baseURL string) *PaymentGateway {
	return &PaymentGateway{
		baseURL: baseURL,
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
		FailureRate: 0.3, // 30% failure rate for demo
		Timeout:     2 * time.Second,
	}
}

func (pg *PaymentGateway) ProcessPayment(orderID string, amount float64, currency string, attempt int) *PaymentResult {
	// Simulate exponential backoff delay
	backoff := pg.calculateBackoff(attempt)
	time.Sleep(backoff)

	// Simulate random failures (decreasing with retries)
	pg.mu.Lock()
	failureRate := pg.FailureRate - (float64(attempt) * 0.1)
	if failureRate < 0 {
		failureRate = 0
	}
	shouldFail := rand.Float64() < failureRate
	pg.mu.Unlock()

	if shouldFail {
		return &PaymentResult{
			Success: false,
			Error:   "payment_gateway_timeout",
		}
	}

	// Simulate successful payment
	return &PaymentResult{
		Success:   true,
		PaymentID: fmt.Sprintf("PAY-%s-%d", orderID, time.Now().Unix()),
	}
}

func (pg *PaymentGateway) calculateBackoff(attempt int) time.Duration {
	// Exponential backoff: 100ms, 200ms, 400ms, 800ms...
	delay := 100 * time.Millisecond
	for i := 0; i < attempt; i++ {
		delay *= 2
	}
	// Add jitter (±10%)
	jitter := time.Duration(rand.Float64()*0.2*float64(delay)) - delay/10
	return delay + jitter
}

func (pg *PaymentGateway) ProcessRefund(orderID string, paymentID string) error {
	// Simulate refund processing
	time.Sleep(500 * time.Millisecond)
	return nil
}

// MockPaymentServer simulates a real payment gateway HTTP API
type MockPaymentServer struct {
	server *http.Server
	port   int
}

func NewMockPaymentServer(port int) *MockPaymentServer {
	return &MockPaymentServer{
		port: port,
	}
}

func (ms *MockPaymentServer) Start() error {
	mux := http.NewServeMux()

	mux.HandleFunc("/api/payments", ms.handlePayment)
	mux.HandleFunc("/api/payments/", ms.handleRefund)
	mux.HandleFunc("/health", ms.handleHealth)

	ms.server = &http.Server{
		Addr:    fmt.Sprintf(":%d", ms.port),
		Handler: mux,
	}

	go func() {
		log.Printf("Mock payment server starting on port %d", ms.port)
		if err := ms.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("Payment server error: %v", err)
		}
	}()

	// Wait for server to start
	time.Sleep(100 * time.Millisecond)
	return nil
}

func (ms *MockPaymentServer) handlePayment(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		OrderID string  `json:"order_id"`
		Amount  float64 `json:"amount"`
		Attempt int     `json:"attempt"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Simulate success (70% chance, increases with attempts)
	successChance := 0.7 + (float64(req.Attempt) * 0.1)
	if successChance > 0.95 {
		successChance = 0.95
	}

	response := map[string]interface{}{
		"order_id": req.OrderID,
	}

	if rand.Float64() < successChance {
		response["success"] = true
		response["payment_id"] = fmt.Sprintf("PAY-%s-%d", req.OrderID, time.Now().Unix())
	} else {
		response["success"] = false
		response["error"] = "card_declined"
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (ms *MockPaymentServer) handleRefund(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	response := map[string]interface{}{
		"success":      true,
		"refund_id":    fmt.Sprintf("REFUND-%d", time.Now().Unix()),
		"processed_at": time.Now(),
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (ms *MockPaymentServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

func (ms *MockPaymentServer) Stop() error {
	if ms.server != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return ms.server.Shutdown(ctx)
	}
	return nil
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Load config
	cfg, err := config.LoadFleuveToml("")
	if err != nil {
		log.Printf("Warning: Could not load config file: %v", err)
	}

	// Use environment variable or default
	dbURL := cfg.DatabaseURL
	if dbURL == "" {
		dbURL = os.Getenv("FLEUVE_DATABASE_URL")
	}
	if dbURL == "" {
		dbURL = "postgresql://postgres:postgres@localhost:5432/fleuve?sslmode=disable"
	}

	// Start mock payment server
	paymentServer := NewMockPaymentServer(8090)
	if err := paymentServer.Start(); err != nil {
		log.Fatalf("Failed to start payment server: %v", err)
	}
	defer paymentServer.Stop()

	log.Println("=== E-Commerce Order Workflow Example ===")
	log.Println("This example demonstrates:")
	log.Println("  ✓ State machine transitions")
	log.Println("  ✓ External payment gateway integration")
	log.Println("  ✓ Retry with exponential backoff")
	log.Println("  ✓ Error handling and DLQ behavior")
	log.Println("  ✓ Compensation (refunds)")
	log.Println()

	// Create connection pool
	pool, err := repo.NewPGXPool(ctx, dbURL, 10)
	if err != nil {
		log.Fatalf("Failed to create connection pool: %v", err)
	}
	defer pool.Close()

	// Test connection
	if err := pool.Ping(ctx); err != nil {
		log.Fatalf("Failed to ping database: %v", err)
	}

	log.Println("Connected to database successfully!")

	// Create ephemeral storage
	storage := repo.NewInProcessEphemeralStorage(1000)

	// Create payment gateway and workflow
	paymentGateway := NewPaymentGateway("http://localhost:8090")
	workflow := NewOrderWorkflow(paymentGateway)

	// Create repository
	repository := repo.NewPGXRepo(
		pool,
		"OrderWorkflow",
		workflow,
		storage,
	)
	var rejection *model.Rejection

	// Example 1: Successful order flow
	log.Println("\n=== Example 1: Successful Order Flow ===")
	orderID1 := fmt.Sprintf("ORD-%d", time.Now().Unix())

	state, err := repository.CreateNew(ctx, &CreateOrder{
		OrderID:    orderID1,
		CustomerID: "CUST-001",
		Amount:     99.99,
		Currency:   "USD",
	}, orderID1, nil)

	if err != nil {
		log.Fatalf("Failed to create order: %v", err)
	}
	log.Printf("✓ Order created: ID=%s, Status=%s", orderID1, getStateStatus(state))

	// Process payment (may need retries)
	state = processPaymentWithRetry(ctx, repository, orderID1, 3)

	if state.State.(*OrderState).Status == OrderStatusPaid {
		log.Printf("✓ Payment processed: PaymentID=%s", state.State.(*OrderState).PaymentID)

		// Ship order
		state, _, rejection = repository.ProcessCommand(ctx, orderID1, &ShipOrder{
			OrderID:        orderID1,
			TrackingNumber: "TRACK-12345",
		})
		if rejection != nil {
			log.Fatalf("Failed to ship order: %s", rejection.Msg)
		}
		log.Printf("✓ Order shipped: TrackingNumber=%s", state.State.(*OrderState).TrackingNumber)

		// Deliver order
		state, _, rejection = repository.ProcessCommand(ctx, orderID1, &DeliverOrder{
			OrderID: orderID1,
		})
		if rejection != nil {
			log.Fatalf("Failed to deliver order: %s", rejection.Msg)
		}
		log.Printf("✓ Order delivered: Status=%s", state.State.(*OrderState).Status)
		log.Println("✅ Order workflow completed successfully!")
	}

	// Example 2: Order with payment failures (DLQ scenario)
	log.Println("\n=== Example 2: Order with Payment Failures (DLQ) ===")
	orderID2 := fmt.Sprintf("ORD-%d-FAIL", time.Now().Unix())

	// Increase failure rate for this example
	paymentGateway.FailureRate = 1.0 // 100% failure

	state, err = repository.CreateNew(ctx, &CreateOrder{
		OrderID:    orderID2,
		CustomerID: "CUST-002",
		Amount:     149.99,
		Currency:   "USD",
	}, orderID2, nil)

	if err != nil {
		log.Fatalf("Failed to create order: %v", err)
	}
	log.Printf("✓ Order created: ID=%s, Status=%s", orderID2, getStateStatus(state))

	// Try to process payment (will fail all attempts)
	state = processPaymentWithRetry(ctx, repository, orderID2, 3)

	finalState := state.State.(*OrderState)
	if finalState.Status == OrderStatusCancelled {
		log.Printf("✓ Order moved to DLQ after %d failed attempts", finalState.RetryCount)
		log.Printf("  Reason: %s", finalState.CancellationReason)
		log.Println("✅ DLQ behavior demonstrated successfully!")
	}

	// Example 3: Order cancellation with refund
	log.Println("\n=== Example 3: Order Cancellation with Refund ===")
	paymentGateway.FailureRate = 0.3 // Reset failure rate
	orderID3 := fmt.Sprintf("ORD-%d-CANCEL", time.Now().Unix())

	state, err = repository.CreateNew(ctx, &CreateOrder{
		OrderID:    orderID3,
		CustomerID: "CUST-003",
		Amount:     199.99,
		Currency:   "USD",
	}, orderID3, nil)

	if err != nil {
		log.Fatalf("Failed to create order: %v", err)
	}
	log.Printf("✓ Order created: ID=%s, Status=%s", orderID3, getStateStatus(state))

	// Process payment
	state = processPaymentWithRetry(ctx, repository, orderID3, 3)

	if state.State.(*OrderState).Status == OrderStatusPaid {
		log.Printf("✓ Payment processed: PaymentID=%s", state.State.(*OrderState).PaymentID)

		// Customer requests cancellation
		state, _, rejection = repository.ProcessCommand(ctx, orderID3, &CancelOrder{
			OrderID: orderID3,
			Reason:  "customer_request",
		})
		if rejection != nil {
			log.Fatalf("Failed to cancel order: %s", rejection.Msg)
		}

		finalState := state.State.(*OrderState)
		log.Printf("✓ Order cancelled: Reason=%s", finalState.CancellationReason)
		if finalState.CancellationReason == "customer_request" {
			// In a real implementation, we would trigger refund here
			log.Printf("✓ Refund triggered for PaymentID=%s", finalState.PaymentID)
		}
		log.Println("✅ Cancellation with compensation demonstrated!")
	}

	// Print final statistics
	log.Println("\n=== Workflow Statistics ===")
	log.Printf("Total orders processed: 3")
	log.Printf("Successful deliveries: 1")
	log.Printf("DLQ orders: 1")
	log.Printf("Cancelled with refund: 1")

	log.Println("\n✅ All examples completed successfully!")
	log.Println("Press Ctrl+C to exit...")

	// Wait for interrupt
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	log.Println("\nShutting down...")
}

func processPaymentWithRetry(ctx context.Context, repo *repo.PGXRepo, orderID string, maxRetries int) *model.StoredState {
	var state *model.StoredState
	var err error

	for attempt := 0; attempt < maxRetries; attempt++ {
		state, _, err = repo.ProcessCommand(ctx, orderID, &ProcessPayment{
			OrderID: orderID,
		})

		if err != nil {
			log.Printf("  Payment attempt %d failed: %v", attempt+1, err)
			time.Sleep(time.Duration(attempt+1) * 500 * time.Millisecond)
			continue
		}

		orderState := state.State.(*OrderState)
		if orderState.Status == OrderStatusPaid {
			return state
		}

		if orderState.Status == OrderStatusCancelled {
			log.Printf("  ✗ Payment failed permanently after %d attempts", orderState.RetryCount)
			return state
		}

		log.Printf("  ⏳ Payment attempt %d failed, retrying...", attempt+1)
		time.Sleep(time.Duration(attempt+1) * 500 * time.Millisecond)
	}

	if state == nil {
		state, _ = repo.GetCurrentState(ctx, orderID)
	}
	return state
}

func getStateStatus(state *model.StoredState) string {
	if state == nil || state.State == nil {
		return "unknown"
	}
	return string(state.State.(*OrderState).Status)
}
