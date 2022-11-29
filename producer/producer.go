package producer

import (
	j "encoding/json"
	"io"
	"os"

	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
)

// json encoder implementation
func json(msg interface{}, wr io.Writer) error {
	enc := j.NewEncoder(wr)
	return enc.Encode(msg)
}

type KafkaProducer struct {
	logger         Loggerer
	config         *sarama.Config
	producer       sarama.AsyncProducer
	topic          string
	encoder        EncoderFn
	errorHandler   KafkaErrorHandler
	successHandler KafkaSuccessHandler
}

func NewKafkaProducer(brokerList []string, topic string, opts ...Option) (*KafkaProducer, error) {
	l := zerolog.New(os.Stdout)
	conf := &options{config: sarama.NewConfig(), logger: &l, encoder: json}
	for _, opt := range opts {
		opt(conf)
	}

	producer, err := newAsyncProducer(brokerList, conf.config)
	if err != nil {
		return nil, err
	}

	stream := &KafkaProducer{
		logger:         conf.logger,
		config:         conf.config,
		producer:       producer,
		topic:          topic,
		encoder:        conf.encoder,
		errorHandler:   conf.errorHandler,
		successHandler: conf.successHandler,
	}

	if conf.config.Producer.Return.Errors || conf.config.Producer.Return.Successes {
		if stream.errorHandler != nil || stream.successHandler != nil {
			go stream.runMsgProcessor()
		}
	}
	return stream, nil
}

func (s *KafkaProducer) runMsgProcessor() {
	successHandler := s.successHandler
	if successHandler == nil {
		successHandler = func(msg *sarama.ProducerMessage) {
			s.logger.Info().Msgf("[kafka] %s [%s] success partition=%d offset=%d\n",
				msg.Timestamp, msg.Topic, msg.Partition, msg.Offset)
		}
	}

	errorHandler := s.errorHandler
	if errorHandler == nil {
		errorHandler = func(err *sarama.ProducerError) {
			s.logger.Err(err).Msgf("[kafka] time:%s , topic:%s", err.Msg.Timestamp, err.Msg.Topic)
		}
	}

loop:
	for {
		select {
		case errMsg, ok := <-s.producer.Errors():
			if !ok {
				break loop
			}
			errorHandler(errMsg)
		case msg, ok := <-s.producer.Successes():
			if !ok {
				break loop
			}
			successHandler(msg)
			if msg.Value != nil {
				msg.Value.(kafkaByteEncoder).Release()
			}
		}
	}
}

func newAsyncProducer(brokers []string, conf *sarama.Config) (sarama.AsyncProducer, error) {
	if conf == nil {
		conf = sarama.NewConfig()
	}

	producer, err := sarama.NewAsyncProducer(brokers, conf)
	if err != nil {
		return nil, errors.Wrap(err, "[kafka] failed to start async producer")
	}
	return producer, nil
}

// Key is used for sending a message to particular partirion if that's required, if it's not set Round Robin will be
// implemented while message distribution
func (s *KafkaProducer) Send(key string, message interface{}) error {
	data, err := s.encodeMessage(message)
	if err != nil {
		return errors.Wrap(err, "[kafka] can't encode message")
	}

	s.producer.Input() <- &sarama.ProducerMessage{
		Topic: s.topic,
		Key:   kafkaByteEncoder(key),
		Value: kafkaByteEncoder(data),
	}
	return nil
}

func (s *KafkaProducer) encodeMessage(msg interface{}) ([]byte, error) {
	buf := acquireBuffer()
	if err := s.encoder(msg, buf); err != nil {
		releaseBuffer(buf)
		return nil, err
	}
	result := buf.Bytes()
	releaseBuffer(buf)
	return result, nil
}

func (s *KafkaProducer) Close() error {
	return s.producer.Close()
}

func (s *KafkaProducer) Producer() sarama.AsyncProducer {
	return s.producer
}
