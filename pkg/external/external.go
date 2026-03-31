package external

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/doomervibe/fleuve-go/pkg/model"
)

type ExternalMessageConsumer struct {
	natsClient        interface{}
	streamName        string
	consumerName      string
	workflowType      string
	workflowTypeClass model.Workflow
	repo              interface{}
	sessionMaker      interface{}
	parsePayload      func([]byte) (model.Command, error)
	wfIDRule          func(string) bool
	running           bool
	mu                sync.Mutex
	ctx               context.Context
	cancel            context.CancelFunc
}

type ExternalMessageConsumerOption func(*ExternalMessageConsumer)

func NewExternalMessageConsumer(opts ...ExternalMessageConsumerOption) *ExternalMessageConsumer {
	c := &ExternalMessageConsumer{
		running: false,
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

func WithNATSClient(client interface{}) ExternalMessageConsumerOption {
	return func(c *ExternalMessageConsumer) { c.natsClient = client }
}

func WithStreamName(name string) ExternalMessageConsumerOption {
	return func(c *ExternalMessageConsumer) { c.streamName = name }
}

func WithConsumerName(name string) ExternalMessageConsumerOption {
	return func(c *ExternalMessageConsumer) { c.consumerName = name }
}

func WithWorkflowType(wt string, wf model.Workflow) ExternalMessageConsumerOption {
	return func(c *ExternalMessageConsumer) { c.workflowType = wt; c.workflowTypeClass = wf }
}

func WithRepo(repo interface{}) ExternalMessageConsumerOption {
	return func(c *ExternalMessageConsumer) { c.repo = repo }
}

func WithSessionMaker(sm interface{}) ExternalMessageConsumerOption {
	return func(c *ExternalMessageConsumer) { c.sessionMaker = sm }
}

func WithParsePayload(fn func([]byte) (model.Command, error)) ExternalMessageConsumerOption {
	return func(c *ExternalMessageConsumer) { c.parsePayload = fn }
}

func WithWFIDRule(rule func(string) bool) ExternalMessageConsumerOption {
	return func(c *ExternalMessageConsumer) { c.wfIDRule = rule }
}

func (c *ExternalMessageConsumer) Start(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.running {
		return nil
	}

	c.ctx, c.cancel = context.WithCancel(ctx)
	c.running = true

	go c.consumeLoop()

	return nil
}

func (c *ExternalMessageConsumer) Stop() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.running = false
	if c.cancel != nil {
		c.cancel()
	}

	return nil
}

func (c *ExternalMessageConsumer) consumeLoop() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			c.pollMessages()
		}
	}
}

func (c *ExternalMessageConsumer) pollMessages() {
	// Stub: read configured deps so options stay live until NATS wiring lands.
	_, _, _, _ = c.natsClient, c.repo, c.sessionMaker, c.consumerName
	_ = c.streamName + c.workflowType
	if c.workflowTypeClass != nil {
		_, _ = c.workflowTypeClass.Name(), c.workflowTypeClass.SchemaVersion()
	}
	if c.wfIDRule != nil {
		_ = c.wfIDRule("")
	}
}

type ExternalMessage struct {
	Topic     string          `json:"topic"`
	Payload   json.RawMessage `json:"payload"`
	Timestamp time.Time       `json:"timestamp"`
	Metadata  map[string]any  `json:"metadata"`
}

func (c *ExternalMessageConsumer) ProcessMessage(ctx context.Context, topic string, payload []byte) error {
	if c.parsePayload == nil {
		return nil
	}

	cmd, err := c.parsePayload(payload)
	if err != nil {
		log.Printf("Failed to parse external message payload: %v", err)
		return err
	}

	_ = cmd

	return nil
}

func (c *ExternalMessageConsumer) GetSubscribedTopics(ctx context.Context) ([]string, error) {
	return []string{}, nil
}

type OutboxPublisher struct {
	batchSize    int
	pollInterval time.Duration
	running      bool
	ctx          context.Context
	cancel       context.CancelFunc
}

type OutboxPublisherOption func(*OutboxPublisher)

func NewOutboxPublisher(opts ...OutboxPublisherOption) *OutboxPublisher {
	p := &OutboxPublisher{
		batchSize:    100,
		pollInterval: 100 * time.Millisecond,
		running:      false,
	}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

func WithOutboxBatchSize(n int) OutboxPublisherOption {
	return func(p *OutboxPublisher) { p.batchSize = n }
}

func WithOutboxPollInterval(d time.Duration) OutboxPublisherOption {
	return func(p *OutboxPublisher) { p.pollInterval = d }
}

func (p *OutboxPublisher) Start(ctx context.Context) error {
	p.ctx, p.cancel = context.WithCancel(ctx)
	p.running = true
	go p.publishLoop()
	return nil
}

func (p *OutboxPublisher) Stop() error {
	p.running = false
	if p.cancel != nil {
		p.cancel()
	}
	return nil
}

func (p *OutboxPublisher) publishLoop() {
	ticker := time.NewTicker(p.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-p.ctx.Done():
			return
		case <-ticker.C:
			p.publishBatch()
		}
	}
}

func (p *OutboxPublisher) publishBatch() {
}

func (p *OutboxPublisher) PublishEvent(ctx context.Context, event interface{}) error {
	_ = event
	return nil
}
