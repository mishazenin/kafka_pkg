package producer

import (
	"github.com/Shopify/sarama"
)

type KafkaErrorHandler func(*sarama.ProducerError)
type KafkaSuccessHandler func(*sarama.ProducerMessage)
