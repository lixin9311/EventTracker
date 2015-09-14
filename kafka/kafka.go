package kafka

import (
	"github.com/Shopify/sarama"
	"io"
	"log"
	"strings"
	"sync"
)

const (
	buffersize = 128
)

var (
	producer   sarama.SyncProducer
	topic      = map[string]string{}
	partition  = int32(-1)
	brokerlist = ""
	logger     *log.Logger
)

func try(fun func(), err_handler func(interface{})) {
	defer func() {
		if err := recover(); err != nil {
			err_handler(err)
		}
	}()
	fun()
}

// Init initialize the kafka package
func Init(w io.Writer, setting map[string]interface{}) {
	var err error
	logger = log.New(w, "[kafka]:", log.LstdFlags|log.Lshortfile)
	config := sarama.NewConfig()
	// init partitioner
	switch setting["partitioner"].(string) {
	case "hash":
		config.Producer.Partitioner = sarama.NewHashPartitioner
	case "random":
		config.Producer.Partitioner = sarama.NewRandomPartitioner
	case "manual":
		config.Producer.Partitioner = sarama.NewManualPartitioner
		if int(setting["partition"].(float64)) == -1 {
			logger.Fatalln("Partition is required when partitioning manually.")
		}
	default:
		logger.Fatalf("Partitioner %s not supported.\n", setting["partitioner"].(string))
	}
	partition = int32(setting["partition"].(float64))
	// init topic
	try(func() {
		for k, v := range setting["topic"].(map[string]interface{}) {
			topic[k] = v.(string)
		}
	}, func(e interface{}) {
		logger.Fatalln("Failed to parse topic from config:", e)
	})
	config.Producer.RequiredAcks = sarama.WaitForAll
	// init producer
	brokerlist = setting["brokers"].(string)
	producer, err = sarama.NewSyncProducer(strings.Split(setting["brokers"].(string), ","), config)
	if err != nil {
		logger.Fatalln("Init failed:", err)
	}
	logger.Println("Init completed")
}

// SendByteMessage sends a byte slice message to kafka
func SendByteMessage(msg []byte, event_type string) (partition int32, offset int64, err error) {
	if _, ok := topic[event_type]; !ok {
		event_type = "default"
	}
	message := &sarama.ProducerMessage{Topic: topic[event_type], Partition: partition}
	message.Value = sarama.ByteEncoder(msg)
	return producer.SendMessage(message)
}

// SendStringMessage sends a string message to kafka
func SendStringMessage(msg string, event_type string) (partition int32, offset int64, err error) {
	if _, ok := topic[event_type]; !ok {
		event_type = "default"
	}
	message := &sarama.ProducerMessage{Topic: topic[event_type], Partition: partition}
	message.Value = sarama.StringEncoder(msg)
	return producer.SendMessage(message)
}

// Destroy closes kafka pruducer
func Destroy() {
	err := producer.Close()
	if err != nil {
		logger.Println("failed to close producer gracefully:", err)
	}
}

func Consumer(topic string, messages chan<- []byte, closing <-chan struct{}) {
	var wg sync.WaitGroup
	consumer, err := sarama.NewConsumer(strings.Split(brokerlist, ","), nil)
	if err != nil {
		logger.Println("Failed to create consumer:", err)
		return
	}
	partitionList, err := consumer.Partitions(topic)
	if err != nil {
		logger.Println("Failed to get partitions:", err)
		return
	}
	for _, partition := range partitionList {
		pc, err := consumer.ConsumePartition(topic, partition, sarama.OffsetNewest)
		if err != nil {
			log.Println("Failed to create partition consumer:", err)
			return
		}
		go func(pc sarama.PartitionConsumer) {
			defer wg.Done()
			<-closing
			pc.AsyncClose()
		}(pc)
		wg.Add(1)
		go func(pc sarama.PartitionConsumer) {
			for message := range pc.Messages() {
				messages <- message.Value
			}
		}(pc)
	}
	wg.Wait()
}
