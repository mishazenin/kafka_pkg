package consumer

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Shopify/sarama"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
)

const cancelTimeout = time.Second * 50

type Worker struct {
	logger     logger
	ctx        context.Context
	client     sarama.Client
	topics     []string
	kafkaGroup string
	batchSize  int
	readSince  time.Time

	destination chan *KafkaMsg

	// use a consumer as a consumer group (brokers keep offset for each consumer group)
	keepOffset bool
	osSignals  []os.Signal
	shutdownCh chan os.Signal

	builder msgBuilder
}

func New(opts ...Option) *Worker {

	log := zerolog.New(zerolog.NewConsoleWriter())
	o := &options{
		batchSize:       10000,
		topics:          []string{"producer-category-table-testing", "producer-image-table-testing", "producer-product-table-testing"},
		kafkaGroup:      "",
		readSince:       time.Now().Add(time.Hour * 60 * -1),
		ctx:             context.Background(),
		logger:          &log,
		shutdownSignals: []os.Signal{syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT},
	}

	for _, opt := range opts {
		opt(o)
	}

	return &Worker{
		topics:      o.topics,
		logger:      o.logger,
		ctx:         o.ctx,
		client:      o.client,
		kafkaGroup:  o.kafkaGroup,
		batchSize:   o.batchSize,
		readSince:   o.readSince,
		destination: o.dest,
		osSignals:   o.shutdownSignals,
		builder:     o.builderFn,
		shutdownCh:  make(chan os.Signal),
	}
}

func (w *Worker) Run() error {
	consumer, err := sarama.NewConsumerGroupFromClient(w.kafkaGroup, w.client)
	if err != nil {
		w.logger.Err(err).Msg("[kafka] can't create consumer group client")
		return fmt.Errorf("[kafka] can't create consumer group client")
	}

	conf := &HandlerConfig{
		BatchSize: w.batchSize,
		ReadSince: w.readSince,
	}

	handler := newMsgHandler(w.destination, conf, w.logger, w.builder)
	consHandler := newConsumerHandler(handler, w.keepOffset, w.logger)
	if err != nil {
		w.logger.Err(err).Msg("[kafka] failed to consume partition")
	}

	// context for every connect/event consumption
	ctx, cancel := context.WithCancel(w.ctx)
	errCh := make(chan error)
	go func(errCh chan error) {
		for {
			select {
			case err, ok := <-consumer.Errors():
				if !ok {
					continue
				}
				w.logger.Err(err).Msg("[kafka] consumer error")
				Instance().TotalDuration.With(prometheus.Labels{"reason": err.Error(), "step": "consumer error"})
			default:
				if err := consumer.Consume(ctx, w.topics, consHandler); err != nil {
					w.logger.Err(err).Msg("[kafka] error during Consuming")
					errCh <- err
				}
			}
		}
	}(errCh)

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, w.osSignals...)

	w.logger.Info().Msg("[kafka] consumer running...")
	select {
	case err := <-errCh:
		w.logger.Err(err).Msg("")
		w.shutdownCh <- syscall.SIGQUIT
	case sig := <-signals:
		w.logger.Info().Msgf("[kafka] terminating: %v signal received", sig)
		w.shutdownCh <- syscall.SIGQUIT
	case <-ctx.Done():
		w.logger.Info().Msg("[kafka] terminating: context canceled")
		w.shutdownCh <- syscall.SIGQUIT
	}

	<-w.shutdownCh
	w.logger.Info().Msg("[kafka] osSignals...")
	done := make(chan struct{})
	go func() {
		cancel()
		go func() {
			_ = consumer.Close()
		}()
		handler.closeQueue()
		done <- struct{}{}
	}()

	select {
	case <-done:
		w.logger.Info().Msg("[kafka] Done! it's closed")
		return nil
	case <-time.After(cancelTimeout):
		w.logger.Warn().Msg("[kafka] canceled by cancelTimeout")
		return fmt.Errorf("[kafka] cancelTimeout error")
	}
}
