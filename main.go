package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/Shopify/sarama"
	"github.com/google/uuid"
	"kafka/consumer"
	"kafka/producer"
)

type Payload struct {
	ID        int    `json:"id"`
	Url       string `json:"url"`
	ProductId int    `json:"product_id"`
	TimeWrite time.Time
}

type Event struct {
	Payload Payload
	// kafka info
	KafkaTopic     string
	KafkaOffSet    int64
	KafkaPartition int32
}

func (e Event) Time() int64 {
	return e.Payload.TimeWrite.Unix()
}

// Converting function
// generic type
func msgToEvent(msg *sarama.ConsumerMessage) (consumer.KafkaMsg, error) {
	var (
		message Event   // struct that's needed to be returned
		payload Payload // struct that's in kafka cluster
	)

	err := json.Unmarshal(msg.Value, &payload)
	if err != nil {
		return Event{}, err
	}
	message.Payload = payload
	message.KafkaPartition = msg.Partition
	message.KafkaTopic = msg.Topic
	message.KafkaOffSet = msg.Offset
	return message, err
}

func main() {

	toKafka := Event{
		Payload: Payload{
			47,
			"http://google.com/image6.jpg",
			88,
			time.Now().AddDate(0, 0, -5),
		},
		KafkaTopic:     "",
		KafkaOffSet:    0,
		KafkaPartition: 0,
	}
	// toKafka := struct {
	// 	ID        int    `json:"id"`
	// 	URL       string `json:"url"`
	// 	ProductID int    `json:"product_id"`
	// 	Time      int64
	// }{90, "http://google.com/image6.jpg", 47, time.Now().AddDate(0, 0, -5).Unix()}

	p, err := producer.NewKafkaProducer(
		[]string{"localhost:9092"},
		"producer-category-table-testing",
		producer.SuccessHandler(func(msg *sarama.ProducerMessage) {
			fmt.Printf("Successfully sent message to topic:%s , partition:%d , value: %v\n", msg.Topic, msg.Partition, msg.Value)
		}),
		producer.ErrorHandler(func(msg *sarama.ProducerError) {
			fmt.Printf("Bro, you message: %v faild: %s\n", msg.Msg, msg.Err)
		}),
	)
	if err != nil {
		return
	}

	err = p.Send("", toKafka)
	if err != nil {
		fmt.Printf("failed to send msg : %w", err)
		return
	}

	dest := make(chan *consumer.KafkaMsg)
	ctx := context.Background()
	cl, err := NewKafkaClient(
		SessionId(uuid.New().String()),
		KafkaVersion("3.2.0"),
		Service("test_service"),
		WorkerHeartBeatInterval(time.Second*2),
		KafkaBrokers("localhost:9092"),
	)

	cons := consumer.New(
		consumer.ReadSince(time.Now().AddDate(0, 0, -1)),
		consumer.KeepOffset(false),
		consumer.Context(context.Background()),
		consumer.LoggerSet(nil),
		consumer.Client(cl),
		consumer.Topics([]string{"producer-category-table-testing"}),
		consumer.Group("test"),
		consumer.DestinationChan(dest),
		consumer.BuilderFn(msgToEvent),
	)

	go func() {
		for {
			select {
			case event := <-dest:
				e := *event
				fmt.Println(e.(Event).Payload)
			case <-ctx.Done():
				fmt.Println("context is dead")
			default:

			}
		}
	}()

	if cons.Run() != nil {
		fmt.Println("error from cons")
	}
}
