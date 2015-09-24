package eventtracker

import (
	"github.com/Shopify/sarama"
	"github.com/lixin9311/logrus"
	"github.com/wvanbergen/kafka/consumergroup"
	"github.com/wvanbergen/kazoo-go"
	"time"
)

const (
	buffersize = 128
)

type Kafka struct {
	producer   sarama.SyncProducer
	topic      map[string]string
	partition  int32
	brokerlist []string
	logger     *logrus.Logger
}

func NewKafkaInst(w *logrus.Logger, conf kafka_config) *Kafka {
	var err error
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
			w.WithFields(logrus.Fields{
				"module": "kafka",
			}).Fatalln("Partition is required when partitioning manually.")
		}
	default:
		w.WithFields(logrus.Fields{
			"module": "kafka",
		}).Fatalf("Partitioner %s not supported.\n", conf.Partitioner)
	}
	partition := int32(conf.Partition)
	// init topic
	topic := conf.Topics
	config.Producer.RequiredAcks = sarama.WaitForAll
	// init producer
	brokerlist := conf.Brokers
	producer, err := sarama.NewSyncProducer(brokerlist, config)
	if err != nil {
		w.WithFields(logrus.Fields{
			"module": "kafka",
		}).Fatalln("Init failed:", err)
	}
	w.WithFields(logrus.Fields{
		"module": "kafka",
	}).Infoln("Init completed")
	return &Kafka{producer: producer, topic: topic, partition: partition, brokerlist: brokerlist, logger: w}
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
		self.logger.WithFields(logrus.Fields{
			"module": "kafka",
		}).Infoln("failed to close producer gracefully:", err)
	}
}

func (self *Kafka) NewConsumer(consumerGroup string, topics []string, zoo string) (consumer *consumergroup.ConsumerGroup, err error) {
	var zoos []string
	config := consumergroup.NewConfig()
	config.Offsets.Initial = sarama.OffsetOldest
	config.Offsets.ProcessingTimeout = 10 * time.Second
	zoos, config.Zookeeper.Chroot = kazoo.ParseConnectionString(zoo)
	consumer, err = consumergroup.JoinConsumerGroup(consumerGroup, topics, zoos, config)
	if err != nil {
		return
	}
	return
}
