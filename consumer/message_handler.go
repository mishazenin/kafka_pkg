package consumer

import (
	"fmt"
	"time"

	"github.com/Shopify/sarama"
)

type KafkaMsg interface {
	Time() int64
}

type msgBuilder func(msg *sarama.ConsumerMessage) (KafkaMsg, error)

type msgHandler struct {
	queue     chan *KafkaMsg
	batchSize int
	logger    logger
	builder   msgBuilder
	readSince time.Time
}

type HandlerConfig struct {
	BatchSize int
	ReadSince time.Time
}

func newMsgHandler(ch chan *KafkaMsg, cfg *HandlerConfig, log logger, builder msgBuilder) *msgHandler {
	if cfg.ReadSince.IsZero() {
		log.Error().Msg("[kafka] read since not set")
	}
	h := &msgHandler{
		queue:     ch,
		builder:   builder,
		readSince: cfg.ReadSince,
		logger:    log,
	}
	return h
}
func (h *msgHandler) initQueue() {
	// h.queue = make(chan *any, h.batchSize/2)
}
func (h *msgHandler) closeQueue() {
	close(h.queue)
}

func (h *msgHandler) handle(msg *sarama.ConsumerMessage) error {
	m, err := h.builder(msg)
	if err != nil {
		return err
	}
	if h.readSince.IsZero() || m.Time() > h.readSince.Unix() {
		h.queue <- &m
		return nil
	}
	return fmt.Errorf("message less than set readSince")
}
