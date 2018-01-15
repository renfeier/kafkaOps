package kafkaOps

import (
	"fmt"
	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/golang/protobuf/proto"
	log "github.com/sirupsen/logrus"
	commonapis "gitlab.sz.sensetime.com/viper/commonapis/api"
	"gitlab.sz.sensetime.com/viper/engine-image-process-service/api"
	"strings"
	"time"
)

type KafkaConsumer struct {
	brokers, topic, group string
	consumer              *cluster.Consumer
}

func NewKafkaConsumer(brokers, topic, group string, isFromOldest bool) (*KafkaConsumer, error) {
	log.Debug("[input]:broker:", brokers, " ,topic:", topic, " ,group:", group)

	clusterConfig := cluster.NewConfig()
	clusterConfig.Config.Producer.Return.Successes = true
	if isFromOldest {
		clusterConfig.Config.Consumer.Offsets.Initial = sarama.OffsetOldest
	} else {
		clusterConfig.Config.Consumer.Offsets.Initial = sarama.OffsetNewest
	}

	consumer, err := cluster.NewConsumer(strings.Split(brokers, ","), group, strings.Split(topic, ","), clusterConfig)
	if err != nil {
		log.Errorf("NewConsumer failed:", err)
		return nil, fmt.Errorf("NewConsumer failed")
	}
	log.Debug("NewConsumer successed!")

	kafkaOps := &KafkaConsumer{
		brokers:  brokers,
		topic:    topic,
		group:    group,
		consumer: consumer,
	}
	return kafkaOps, nil
}

func (k *KafkaConsumer) ConsumeOne() (commonapis.ObjectFeature, string, error) {
	if k.consumer == nil {
		log.Fatal("invalid kafkaOps, please new KafkaConsumer first!")
		return commonapis.ObjectFeature{}, "", fmt.Errorf("invalid kafkaOps, please new KafkaConsumer first!")
	}
	select {
	case msg, ok := <-k.consumer.Messages():
		if !ok {
			log.Warn("consume failed")
			return commonapis.ObjectFeature{}, "", fmt.Errorf("consume result failed")
		}
		k.consumer.MarkOffset(msg, "")
		var oresult api.ObjectInfo
		if err := proto.Unmarshal(msg.Value, &oresult); err != nil {
			log.Fatal("unmarshal consume result failed:", err)
			return commonapis.ObjectFeature{}, "", fmt.Errorf("unmarshal consume result failed")
		}
		return *oresult.GetFeature(), oresult.PortraitImage.Url, nil

	case <-time.After(60 * time.Second):
		log.Info("read timeout")
		return commonapis.ObjectFeature{}, "", fmt.Errorf("consume result read timeout")
	}
}

func (k *KafkaConsumer) KafkaConsumerClose() error {
	return k.consumer.Close()
}
