package consumer

import (
	"strconv"

	"github.com/Shopify/sarama"
	"github.com/prometheus/client_golang/prometheus"
)

// consumerHandler represents Sarama consumer consumerHandler
type consumerHandler struct {
	msgHandler *msgHandler
	logger     logger
	keepOffset bool
}

// newConsumerHandler returns new claim consumerHandler (claim = topic + partition)
// usage:
//  1. Setup(sess)
//  2. for claim in claims {
//     go ConsumeClaim(sess, claim)
//     }
//  3. Cleanup(sess)
func newConsumerHandler(msgHandler *msgHandler, offset bool, logger logger) *consumerHandler {
	return &consumerHandler{
		keepOffset: offset,
		msgHandler: msgHandler,
		logger:     logger,
	}
}

// Setup is run at the beginning of a new session, before ConsumeClaim.
// TODO prepare something in setup
func (h *consumerHandler) Setup(sarama.ConsumerGroupSession) error {
	h.msgHandler.initQueue()
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
// Once the Messages() channel is closed, the Handler must finish its processing
// loop and exit.
func (h *consumerHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	// NOTE: Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/Shopify/sarama/blob/master/consumer_group.go#L27-L29
	for msg := range claim.Messages() {
		err := h.msgHandler.handle(msg)
		if err != nil {
			Instance().TotalErrors.With(prometheus.Labels{
				"partition": strconv.Itoa(int(msg.Partition)),
				"topic":     msg.Topic,
				"error":     err.Error(),
			}).Inc()
			h.logger.Err(err).Msg("[kafka] failed to consume a claim")
		}

		Instance().TotalEvents.With(prometheus.Labels{
			"topic":     msg.Topic,
			"partition": strconv.Itoa(int(msg.Partition)),
		}).Inc()

		// kafka keeps offset in its own state for consumer groups only
		if h.keepOffset {
			// committed as read
			session.MarkMessage(msg, "")
		}
	}
	return nil
}

// Cleanup runs at the end of a session, once all ConsumeClaim goroutines have exited
// but before the offsets are committed for the very last time.
func (h *consumerHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	h.msgHandler.closeQueue()
	return nil
}
