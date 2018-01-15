package worker

import (
	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
)

type KafkaProducer struct {
	producer              sarama.AsyncProducer
	brokers, topic, group string
}

func NewKafkaProducer(brokers, topic, group string) (*KafkaProducer, error) {
	log.Debug("[input]:broker:", brokers, " ,topic:", topic, " ,group:", group)
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Timeout = 5 * time.Second
	producer, err := sarama.NewAsyncProducer(strings.Split(brokers, ","), config)
	if err != nil {
		return nil, err
	}
	return &KafkaProducer{
		brokers:  brokers,
		topic:    topic,
		group:    group,
		producer: producer,
	}, nil
}

func (k *KafkaProducer) WriteMessage(topic string, data []byte) error {
	select {
	case k.producer.Input() <- &sarama.ProducerMessage{Topic: topic, Value: sarama.ByteEncoder(data)}:
		return nil
	case err := <-k.producer.Errors():
		return err
	}
}

func (k *KafkaProducer) Close() error {
	return k.producer.Close()
}
