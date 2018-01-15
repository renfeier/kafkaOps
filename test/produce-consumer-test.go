package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	commonapis "gitlab.sz.sensetime.com/viper/commonapis/api"
	engine "gitlab.sz.sensetime.com/viper/engine-image-ingress-service/api"
	"gitlab.sz.sensetime.com/viper/engine-image-process-service/api"
)

func readFile(fn string) []byte {
	d, err := ioutil.ReadFile(fn)
	if err != nil {
		panic(err)
	}
	return d
}

var (
	kafkaAdress   = flag.String("kafka_Address", "localhost:9092", "kafka service adress and endpoint")
	produceNumber = flag.Int("producer_num", 100, "kafka producer number")
	consumeNumber = flag.Int("consume_num", 100, "kafka consumer number")
	itopic        = flag.String("input_topic", "face-engine-data", "kafka input topic")
	otopic        = flag.String("consumer_topic", "face-feature-data", "kafka output topic")
	imgPath       = flag.String("image_path", "test/dataset/face/aynctest/104_20719.jpg", "image path to load")
)

func main() {

	flag.Parse()
	clusterConfig := cluster.NewConfig()
	clusterConfig.Config.Producer.Return.Successes = true
	//	clusterConfig.Config.Consumer.Offsets.Initial = sarama.OffsetOldest

	producer, err := sarama.NewSyncProducer(strings.Split(*kafkaAdress, ","), &clusterConfig.Config)
	if err != nil {
		fmt.Println("NewAsyncProducer failed:", err)
		return
	}
	fmt.Println("NewAsyncProducer successed!")
	fmt.Println("broker:", *kafkaAdress)

	inputTopic := *itopic
	consumerTopic := *otopic
	fmt.Println("inputTopic:", inputTopic)
	fmt.Println("consumerTopic:", consumerTopic)
	consumer, err := cluster.NewConsumer(strings.Split(*kafkaAdress, ","), "test_g1", strings.Split(consumerTopic, ","), clusterConfig)

	if err != nil {
		fmt.Println("NewConsumer failed:", err)
	}
	fmt.Println("NewConsumer successed!")

	start := time.Now()
	k := *produceNumber
	for i := 0; i < k; i++ {
		ed := engine.TargetEngineData{
			CameraInfo: &commonapis.CameraInfo{
				InternalId: &commonapis.CameraIdentifier{CameraIdx: int32(i)}},
			CaptureTime: ptypes.TimestampNow(),
			ReceiveTime: ptypes.TimestampNow(),
			FullImage:   &commonapis.Image{Url: "test.jpg"},
			TargetImages: []*engine.TargetImage{
				&engine.TargetImage{
					Content:    &commonapis.Image{Data: readFile(*imgPath)},
					TargetInfo: &commonapis.TargetAnnotation{},
					ExtraInfo:  "YYY",
				},
			},
			ExtraInfo: "XXX",
		}

		data, err := proto.Marshal(&ed)
		if err != nil {
			fmt.Println("marshal data failed:", err)
		}

		//    _, _, err = producer.SendMessage(&sarama.ProducerMessage{Topic: inputTopic, Value: sarama.ByteEncoder(data)})
		//        if err != nil {
		//            fmt.Println("producer send message failed:",err)
		//        }
		//msg := &sarama.ProducerMessage{Topic: inputTopic, Value: sarama.ByteEncoder(data)}
		msg := &sarama.ProducerMessage{Topic: inputTopic, Value: sarama.ByteEncoder(data)}
		partition, offset, err := producer.SendMessage(msg)
		if err != nil {
			fmt.Printf("FAILED to send message: %s\n", err)
		} else {
			fmt.Printf("> message sent to partition %d at offset %d\n", partition, offset)
		}
	}

	if err := producer.Close(); err != nil {
		fmt.Println("producer close failed:", err)
	}
	fmt.Println("=============producer closed success!==============")
	fmt.Println("=============consuming result begin==================")

	for i := 0; i < *consumeNumber; i++ {
		select {
		case msg, ok := <-consumer.Messages():
			if !ok {
				fmt.Println(i, "consume failed")
			}
			fmt.Println("get message")
			//var oresult engine.TargetEngineData
			var oresult api.ObjectInfo
			if err := proto.Unmarshal(msg.Value, &oresult); err != nil {
				fmt.Println(i, "unmarshal consume result failed:", err)
			}
			fmt.Println("objInfo.panormic data len:", len(oresult.PanoramicImage.Data))
			fmt.Println("objInfo.panoramic img format:", oresult.PanoramicImage.Format)
			fmt.Println("objInfo.panoramic img url:", oresult.PanoramicImage.Url)
			fmt.Println("objInfo.portrait data len:", len(oresult.PortraitImage.Data))
			fmt.Println("objInfo.portrait img format:", oresult.PortraitImage.Format)
			fmt.Println("objInfo.portrait img url:", oresult.PortraitImage.Url)
			fmt.Println("objInfo.objectAnnotaion:")
			fmt.Println(oresult.GetObject())
			fmt.Println("objInfo.Feature version:", oresult.Feature.GetVersion())
			fmt.Println("feature-type:", oresult.GetFeature().GetType())
			fmt.Println("objInfo.Feature len:", len(oresult.Feature.GetBlob()))
			fmt.Println("objInfo.CameraInfo:", oresult.CameraInfo)
			fmt.Println("objInfo.CapturedTime:", oresult.CapturedTime)
			fmt.Println("objInfo.ReceivedTime:", oresult.ReceivedTime)
			fmt.Println("objInfo.ObjectIndexInFrame:", oresult.ObjectIndexInFrame)

		case <-time.After(60 * time.Second):
			fmt.Println("read timeout")
		}
	}
	end := time.Now()
	duration := end.Sub(start)
	fmt.Println("=========time test result============")
	fmt.Printf("total time:%s per %d images\n", duration, k)
	fmt.Printf("image-process async concurrency nums:%f\n", (float64)(k)/duration.Seconds())

}
