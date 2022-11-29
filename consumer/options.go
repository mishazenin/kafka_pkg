package consumer

import (
	"context"
	"os"
	"time"

	"github.com/Shopify/sarama"
)

type Option func(*options)

type options struct {
	batchSize  int
	topics     []string
	kafkaGroup string
	readSince  time.Time

	ctx        context.Context
	dest       chan *KafkaMsg
	client     sarama.Client
	logger     logger
	keepOffset bool

	builderFn       msgBuilder
	shutdownSignals []os.Signal
}

func KeepOffset(keepOffset bool) Option {
	return func(o *options) {
		o.keepOffset = keepOffset
	}
}

func ShutdownSignals(signals []os.Signal) Option {
	return func(o *options) {
		o.shutdownSignals = signals
	}
}

func BuilderFn(fn msgBuilder) Option {
	return func(o *options) {
		o.builderFn = fn
	}
}

func LoggerSet(log logger) Option {
	return func(o *options) {
		o.logger = log
	}
}

func Client(client sarama.Client) Option {
	return func(o *options) {
		o.client = client
	}
}

func DestinationChan(dest chan *KafkaMsg) Option {
	return func(o *options) {
		o.dest = dest
	}
}

func Context(ctx context.Context) Option {
	return func(o *options) {
		o.ctx = ctx
	}
}

func WorkerBatchSize(batchSize int) Option {
	return func(o *options) {
		o.batchSize = batchSize
	}
}

func ReadSince(time time.Time) Option {
	return func(o *options) {
		o.readSince = time
	}
}

func Topics(topics []string) Option {
	return func(o *options) {
		o.topics = topics
	}
}

func Group(group string) Option {
	return func(o *options) {
		o.kafkaGroup = group
	}
}
