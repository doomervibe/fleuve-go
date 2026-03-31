package stream

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"math"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type NATSReader struct {
	nc            *nats.Conn
	js            jetstream.JetStream
	consumer      jetstream.Consumer
	subscription  jetstream.ConsumeContext
	streamName    string
	consumerName  string
	workflowType  string
	batchSize     int
	stopAtOffset  *uint64
	committedSeq  uint64
	lastDelivered uint64
	// lastJSAcked is the highest stream sequence successfully Ack'd to JetStream (contiguous).
	lastJSAcked uint64
	// pendingAcks holds delivered messages until the runner commits offset (Python-style: ack after handling).
	pendingAcks map[uint64]jetstream.Msg
	mu          sync.RWMutex
	ctx         context.Context
	cancel      context.CancelFunc
	offsetPool  *pgxpool.Pool
	offsetReader string
}

type NATSReaderOption func(*NATSReader)

func WithNATSBatchSize(n int) NATSReaderOption {
	return func(r *NATSReader) { r.batchSize = n }
}

func WithNATSStreamName(name string) NATSReaderOption {
	return func(r *NATSReader) { r.streamName = name }
}

func WithNATSConsumerName(name string) NATSReaderOption {
	return func(r *NATSReader) { r.consumerName = name }
}

// WithNATSPersistOffset stores the committed JetStream sequence in the fleuve offsets table
// (same reader key as PGReader) so the consumer can resume after restart.
func WithNATSPersistOffset(pool *pgxpool.Pool, readerKey string) NATSReaderOption {
	return func(r *NATSReader) {
		r.offsetPool = pool
		r.offsetReader = readerKey
	}
}

func NewNATSReader(natsURL, workflowType string, opts ...NATSReaderOption) (*NATSReader, error) {
	nc, err := nats.Connect(natsURL,
		nats.ReconnectWait(2*time.Second),
		nats.MaxReconnects(10),
		nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
			log.Printf("NATS disconnected: %v", err)
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			log.Printf("NATS reconnected to %s", nc.ConnectedUrl())
		}),
	)
	if err != nil {
		return nil, err
	}

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, err
	}

	r := &NATSReader{
		nc:           nc,
		js:           js,
		workflowType: workflowType,
		streamName:   "FLEUVE_" + workflowType,
		consumerName: workflowType + "_consumer",
		batchSize:    100,
		pendingAcks:  make(map[uint64]jetstream.Msg),
	}

	for _, opt := range opts {
		opt(r)
	}

	return r, nil
}

func (r *NATSReader) EnsureStream(ctx context.Context) error {
	_, err := r.js.CreateStream(ctx, jetstream.StreamConfig{
		Name:      r.streamName,
		Subjects:  []string{r.streamName + ".>"},
		Replicas:  1,
		Retention: jetstream.LimitsPolicy,
		MaxAge:    7 * 24 * time.Hour,
	})
	if err != nil {
		if err == jetstream.ErrStreamNameAlreadyInUse {
			return nil
		}
		return err
	}
	return nil
}

func (r *NATSReader) EnsureConsumer(ctx context.Context) error {
	c, err := r.js.CreateConsumer(ctx, r.streamName, jetstream.ConsumerConfig{
		Name:          r.consumerName,
		Durable:       r.consumerName,
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverByStartSequencePolicy,
		OptStartSeq:   r.committedSeq + 1,
	})
	if err != nil {
		if err == jetstream.ErrConsumerNameAlreadyInUse {
			c, err = r.js.Consumer(ctx, r.streamName, r.consumerName)
			if err != nil {
				return err
			}
		} else {
			return err
		}
	}
	r.consumer = c
	return nil
}

func (r *NATSReader) Init(ctx context.Context) error {
	if r.offsetPool != nil && r.offsetReader != "" {
		var o int64
		err := r.offsetPool.QueryRow(ctx, `
			SELECT COALESCE(last_read_event_no, 0) FROM offsets WHERE reader = $1
		`, r.offsetReader).Scan(&o)
		if err == nil && o >= 0 {
			u := uint64(o)
			r.mu.Lock()
			r.committedSeq = u
			r.lastJSAcked = u
			r.mu.Unlock()
		} else if !errors.Is(err, pgx.ErrNoRows) {
			log.Printf("NATS offset load (%s): %v", r.offsetReader, err)
		}
	}
	if err := r.EnsureStream(ctx); err != nil {
		return err
	}
	return r.EnsureConsumer(ctx)
}

func (r *NATSReader) IterEvents(ctx context.Context) <-chan *ConsumedEvent {
	ch := make(chan *ConsumedEvent, 100)

	r.ctx, r.cancel = context.WithCancel(ctx)

	cc, err := r.consumer.Consume(func(msg jetstream.Msg) {
		meta, err := msg.Metadata()
		if err != nil {
			if nakErr := msg.Nak(); nakErr != nil {
				log.Printf("NATS nak after metadata error: %v", nakErr)
			}
			return
		}

		event, err := r.parseMessage(msg)
		if err != nil {
			log.Printf("Failed to parse NATS message: %v", err)
			if nakErr := msg.Nak(); nakErr != nil {
				log.Printf("NATS nak after parse error: %v", nakErr)
			}
			return
		}

		seq := meta.Sequence.Stream
		event.GlobalID = jetStreamSeqToGlobalID(seq)

		r.mu.Lock()
		r.pendingAcks[seq] = msg
		r.lastDelivered = seq
		r.mu.Unlock()

		select {
		case <-r.ctx.Done():
			r.mu.Lock()
			delete(r.pendingAcks, seq)
			r.mu.Unlock()
			if nakErr := msg.Nak(); nakErr != nil {
				log.Printf("NATS nak on shutdown: %v", nakErr)
			}
			return
		case ch <- event:
			// Ack deferred until SetCommittedOffset reaches this sequence (after runner processing).
		}
	}, jetstream.ConsumeErrHandler(func(consumeCtx jetstream.ConsumeContext, err error) {
		log.Printf("NATS consume error: %v", err)
	}))

	if err != nil {
		if r.cancel != nil {
			r.cancel()
			r.cancel = nil
		}
		close(ch)
		log.Printf("Failed to start NATS consumer: %v", err)
		return ch
	}

	r.subscription = cc

	go func() {
		<-r.ctx.Done()
		r.subscription.Stop()
		close(ch)
	}()

	return ch
}

func (r *NATSReader) parseMessage(msg jetstream.Msg) (*ConsumedEvent, error) {
	event, err := UnmarshalConsumedEventMessage(msg.Data())
	if err != nil {
		return nil, err
	}
	if event.Metadata == nil {
		event.Metadata = make(map[string]any)
	}
	for _, h := range msg.Headers() {
		event.Metadata[h[0]] = h[1]
	}
	return event, nil
}

func (r *NATSReader) SetStopAtOffset(offset int64) {
	if offset < 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	seq := uint64(offset) // #nosec G115 -- offset validated non-negative
	r.stopAtOffset = &seq
}

func (r *NATSReader) SetCommittedOffset(offset int64) {
	if offset < 0 {
		return
	}
	target := uint64(offset) // #nosec G115 -- offset validated non-negative

	// Collect messages to ack under lock, then ack outside to avoid holding
	// the mutex during network I/O.
	r.mu.Lock()
	r.committedSeq = target
	var toAck []struct {
		seq uint64
		msg jetstream.Msg
	}
	for r.lastJSAcked < target {
		next := r.lastJSAcked + 1
		msg, ok := r.pendingAcks[next]
		if !ok {
			break
		}
		toAck = append(toAck, struct {
			seq uint64
			msg jetstream.Msg
		}{next, msg})
		delete(r.pendingAcks, next)
		r.lastJSAcked = next
	}
	lastAcked := r.lastJSAcked
	pool := r.offsetPool
	key := r.offsetReader
	r.mu.Unlock()

	// Ack messages outside the lock.
	for _, item := range toAck {
		if err := item.msg.Ack(); err != nil {
			log.Printf("NATS ack seq %d: %v", item.seq, err)
			// Re-insert failed acks so they can be retried.
			r.mu.Lock()
			r.pendingAcks[item.seq] = item.msg
			if item.seq <= r.lastJSAcked {
				r.lastJSAcked = item.seq - 1
			}
			r.mu.Unlock()
			break
		}
	}

	// Persist only JetStream-acked sequences so restarts do not skip messages still pending ack.
	persisted := jetStreamAckedToPersistedOffset(lastAcked)
	if pool != nil && key != "" {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err := pool.Exec(ctx, `
			INSERT INTO offsets (reader, last_read_event_no) VALUES ($1, $2)
			ON CONFLICT (reader) DO UPDATE SET last_read_event_no = EXCLUDED.last_read_event_no, updated_at = NOW()
		`, key, persisted)
		if err != nil {
			log.Printf("NATS offset persist (%s): %v", key, err)
		}
	}
}

func (r *NATSReader) LastReadEventGID() int64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return jetStreamSeqToGlobalID(r.lastDelivered)
}

func (r *NATSReader) Close() error {
	if r.cancel != nil {
		r.cancel()
		r.cancel = nil
	}
	if r.subscription != nil {
		r.subscription.Stop()
	}
	r.mu.Lock()
	pendingSeqs := make([]uint64, 0, len(r.pendingAcks))
	for seq := range r.pendingAcks {
		pendingSeqs = append(pendingSeqs, seq)
	}
	for _, seq := range pendingSeqs {
		msg := r.pendingAcks[seq]
		if err := msg.Nak(); err != nil {
			log.Printf("NATS nak pending seq %d on close: %v", seq, err)
		}
		delete(r.pendingAcks, seq)
	}
	r.mu.Unlock()
	if r.nc != nil {
		r.nc.Close()
	}
	return nil
}

func (r *NATSReader) Publish(ctx context.Context, subject string, event *ConsumedEvent) error {
	data, err := json.Marshal(event)
	if err != nil {
		return err
	}

	_, err = r.js.Publish(ctx, r.streamName+"."+subject, data)
	return err
}

type NATSKVStorage struct {
	kv     jetstream.KeyValue
	bucket string
}

func NewNATSKVStorage(nc *nats.Conn, bucket string) (*NATSKVStorage, error) {
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, err
	}

	kv, err := js.KeyValue(context.Background(), bucket)
	if err != nil {
		kv, err = js.CreateKeyValue(context.Background(), jetstream.KeyValueConfig{
			Bucket:  bucket,
			History: 1,
			TTL:     24 * time.Hour,
		})
		if err != nil {
			return nil, err
		}
	}

	return &NATSKVStorage{kv: kv, bucket: bucket}, nil
}

func (s *NATSKVStorage) Get(ctx context.Context, key string) ([]byte, error) {
	entry, err := s.kv.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	return entry.Value(), nil
}

func (s *NATSKVStorage) Put(ctx context.Context, key string, value []byte) (uint64, error) {
	return s.kv.Put(ctx, key, value)
}

func (s *NATSKVStorage) Delete(ctx context.Context, key string) error {
	return s.kv.Delete(ctx, key)
}

func jetStreamSeqToGlobalID(seq uint64) int64 {
	if seq > uint64(math.MaxInt64) {
		return math.MaxInt64
	}
	return int64(seq)
}

func jetStreamAckedToPersistedOffset(lastJSAcked uint64) int64 {
	if lastJSAcked > uint64(math.MaxInt64) {
		return math.MaxInt64
	}
	return int64(lastJSAcked)
}
