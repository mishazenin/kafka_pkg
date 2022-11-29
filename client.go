package main

import (
	"fmt"
	"os"
	"time"

	"github.com/Shopify/sarama"
	"github.com/google/uuid"
)

func NewKafkaClient(
	opts ...Option,
) (sarama.Client, error) {

	o := &options{
		service:                      "golang_service",
		workerHeartBeatInterval:      time.Second * 3,
		workerBatchSize:              100000,
		workerMaxProcessingTime:      time.Millisecond * 100,
		workerConsumerSessionTimeout: time.Second * 10,
		kafkaVersion:                 "3.2.0",
		consumeReturnError:           true,
		brokers:                      []string{"localhost"},
		sessionId:                    uuid.New().String(),
	}

	for _, opt := range opts {
		opt(o)
	}

	cfg := sarama.NewConfig()
	cfg.ClientID = makeClientID(o.service, o.sessionId)

	version, err := sarama.ParseKafkaVersion(o.kafkaVersion)
	if err != nil {
		return nil, fmt.Errorf("[kafka] could not parse a version")
	}
	cfg.Version = version

	if o.workerConsumerSessionTimeout > 0 {
		cfg.Consumer.Group.Session.Timeout = o.workerConsumerSessionTimeout
	}

	// Heartbeats are used to ensure that the consumer's session stays active and
	// to facilitate re-balancing when new consumers join or leave the group.
	if o.workerHeartBeatInterval > 0 {
		cfg.Consumer.Group.Heartbeat.Interval = o.workerHeartBeatInterval
	}
	if o.workerMaxProcessingTime > 0 {
		cfg.Consumer.MaxProcessingTime = o.workerMaxProcessingTime
	}
	cfg.Consumer.Return.Errors = o.consumeReturnError
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest

	kafkaClient, err := sarama.NewClient(o.brokers, cfg)
	if err != nil {
		return nil, fmt.Errorf("[kafka] could not create a client")
	}
	return kafkaClient, nil
}

func makeClientID(service, sessionId string) string {
	hostname, _ := os.Hostname()
	return service + "_" + hostname + "_" + sessionId
}
