package eventtracker

import (
	"github.com/Shopify/sarama"
	"io"
	"log"
	"sync"
)

const (
	buffersize = 128
)

type Kafka struct {
	producer   sarama.SyncProducer
	topic      map[string]string
	partition  int32
	brokerlist []string
	logger     *log.Logger
}

func NewKafkaInst(w io.Writer, conf kafka_config) *Kafka {
	var err error
	logger := log.New(w, "[kafka]:", log.LstdFlags|log.Lshortfile)
	config := sarama.NewConfig()
	// init partitioner
	switch conf.Partitioner {
	case "hash":
		config.Producer.Partitioner = sarama.NewHashPartitioner
	case "random":
		config.Producer.Partitioner = sarama.NewRandomPartitioner
	case "manual":
		config.Producer.Partitioner = sarama.NewManualPartitioner
		if conf.Partition == -1 {
			logger.Fatalln("Partition is required when partitioning manually.")
		}
	default:
		logger.Fatalf("Partitioner %s not supported.\n", conf.Partitioner)
	}
	partition := int32(conf.Partition)
	// init topic
	topic := conf.Topics
	config.Producer.RequiredAcks = sarama.WaitForAll
	// init producer
	brokerlist := conf.Brokers
	producer, err := sarama.NewSyncProducer(brokerlist, config)
	if err != nil {
		logger.Fatalln("Init failed:", err)
	}
	logger.Println("Init completed")
	return &Kafka{producer: producer, topic: topic, partition: partition, brokerlist: brokerlist, logger: logger}
}

// SendByteMessage sends a byte slice message to kafka
func (self *Kafka) SendByteMessage(msg []byte, event_type string) (partition int32, offset int64, err error) {
	if _, ok := self.topic[event_type]; !ok {
		event_type = "default"
	}
	message := &sarama.ProducerMessage{Topic: self.topic[event_type], Partition: self.partition}
	message.Value = sarama.ByteEncoder(msg)
	return self.producer.SendMessage(message)
}

// SendStringMessage sends a string message to kafka
func (self *Kafka) SendStringMessage(msg string, event_type string) (partition int32, offset int64, err error) {
	if _, ok := self.topic[event_type]; !ok {
		event_type = "default"
	}
	message := &sarama.ProducerMessage{Topic: self.topic[event_type], Partition: self.partition}
	message.Value = sarama.StringEncoder(msg)
	return self.producer.SendMessage(message)
}

// Destroy closes kafka pruducer
func (self *Kafka) Destroy() {
	err := self.producer.Close()
	if err != nil {
		self.logger.Println("failed to close producer gracefully:", err)
	}
}

func (self *Kafka) NewConsumer(topic string, messages chan<- []byte, closing <-chan struct{}) {
	var wg sync.WaitGroup
	consumer, err := sarama.NewConsumer(self.brokerlist, nil)
	if err != nil {
		self.logger.Println("Failed to create consumer:", err)
		return
	}
	partitionList, err := consumer.Partitions(topic)
	if err != nil {
		self.logger.Println("Failed to get partitions:", err)
		return
	}
	for _, partition := range partitionList {
		pc, err := consumer.ConsumePartition(topic, partition, sarama.OffsetNewest)
		if err != nil {
			self.logger.Println("Failed to create partition consumer:", err)
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
