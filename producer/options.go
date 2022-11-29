package producer

import (
	"time"

	"github.com/Shopify/sarama"
)

type options struct {
	config         *sarama.Config
	logger         Loggerer
	errorHandler   KafkaErrorHandler
	successHandler KafkaSuccessHandler
	encoder        EncoderFn
}

// Option function type
type Option func(conf *options)

func Encoder(enc EncoderFn) Option {
	return func(conf *options) {
		conf.encoder = enc
	}
}

func Logger(logger Loggerer) Option {
	return func(conf *options) {
		conf.logger = logger
	}
}

func Config(config *sarama.Config) Option {
	return func(conf *options) {
		conf.config = config
	}
}

func KafkaVersion(v sarama.KafkaVersion) Option {
	return func(conf *options) {
		conf.config.Version = v
	}
}

func FlushFrequency(frq time.Duration) Option {
	return func(conf *options) {
		conf.config.Producer.Flush.Frequency = frq
	}
}

func FlushMessages(count int) Option {
	return func(conf *options) {
		conf.config.Producer.Flush.MaxMessages = count
	}
}

func Compression(codec sarama.CompressionCodec, level int) Option {
	return func(conf *options) {
		conf.config.Producer.Compression = codec
		conf.config.Producer.CompressionLevel = level
	}
}

func ErrorHandler(h KafkaErrorHandler) Option {
	return func(conf *options) {
		conf.config.Producer.Return.Errors = true
		conf.errorHandler = h
	}
}

func SuccessHandler(h KafkaSuccessHandler) Option {
	return func(conf *options) {
		conf.config.Producer.Return.Successes = true
		conf.successHandler = h
	}
}

func ProducerRetries(count int) Option {
	return func(conf *options) {
		conf.config.Producer.Retry.Max = count
	}
}

func SaramaConfigurator(f func(*sarama.Config)) Option {
	return func(conf *options) {
		f(conf.config)
	}
}
