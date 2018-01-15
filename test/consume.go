package main

import (
	"flag"
	"fmt"
	"gitlab.sz.sensetime.com/viper/engine-image-process-service/kafkaOps"
)

var (
	kafkaAdress   = flag.String("kafka_Address", "localhost:9092", "kafka service adress and endpoint")
	consumeNumber = flag.Int("consumer_num", 2, "kafka consumer number")
	otopic        = flag.String("consumer_topic", "face-feature-data", "kafka output topic")
	groupID       = flag.String("consumer_group_id", "test_g1", "kafka consume group id")
	isFromOldest  = flag.Bool("consumer_isFromOldest", false, "consume kafka result from oldest or newest")
)

func main() {
	flag.Parse()

	kafkaops, err := kafkaOps.NewKafkaOps(*kafkaAdress, *otopic, *groupID, *isFromOldest)
	if err != nil {
		fmt.Println("new kafkaOps failed:", err)
		return
	}
	fmt.Println("new kafkaOps success")

	for i := 0; i < *consumeNumber; i++ {
		feature, smallUrl, err := kafkaops.ConsumeOne()
		if err != nil {
			fmt.Println(i, "ConsumeOne failed:", err)
			continue
		}
		fmt.Println("samllUrl:", smallUrl)
		fmt.Println("feature version:", feature.GetVersion())
		fmt.Println("feature type:", feature.GetType())
		fmt.Println("feature len:", len(feature.GetBlob()))
	}
	fmt.Println("test finished")
}
