package main

import "time"

type Option func(*options)

type options struct {
	service                      string
	workerHeartBeatInterval      time.Duration
	workerBatchSize              int
	workerMaxProcessingTime      time.Duration
	workerConsumerSessionTimeout time.Duration
	kafkaVersion                 string
	consumeReturnError           bool
	brokers                      []string
	sessionId                    string
}

func SessionId(sessionId string) Option {
	return func(o *options) {
		o.sessionId = sessionId
	}
}

func Service(service string) Option {
	return func(o *options) {
		o.service = service
	}
}

func WorkerHeartBeatInterval(interval time.Duration) Option {
	return func(o *options) {
		o.workerHeartBeatInterval = interval
	}
}

func WorkerBatchSize(batchSize int) Option {
	return func(o *options) {
		o.workerBatchSize = batchSize
	}
}

func WorkerMaxProcessingTime(processTime time.Duration) Option {
	return func(o *options) {
		o.workerMaxProcessingTime = processTime
	}
}

func WorkerConsumerSessionTimeout(timeout time.Duration) Option {
	return func(o *options) {
		o.workerConsumerSessionTimeout = timeout
	}
}

func KafkaVersion(version string) Option {
	return func(o *options) {
		o.kafkaVersion = version
	}
}

func ConsumeReturnError(isReturn bool) Option {
	return func(o *options) {
		o.consumeReturnError = isReturn
	}
}

func KafkaBrokers(brokers ...string) Option {
	return func(o *options) {
		o.brokers = brokers
	}
}
