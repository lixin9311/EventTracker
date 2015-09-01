package kafka

import (
	"github.com/Shopify/sarama"
	"io"
	"log"
	"strconv"
	"strings"
)

var (
	producer   sarama.SyncProducer
	topic      = "test"
	partition  = int32(-1)
	brokerlist = "localhost:9092"
	logger     *log.Logger
)

// Init initialize the kafka package
func Init(w io.Writer, setting map[string]string) {
	var err error
	logger = log.New(w, "[kafka]:", log.LstdFlags|log.Lshortfile)
	config := sarama.NewConfig()
	// init partitioner
	switch setting["partitioner"] {
	case "hash":
		config.Producer.Partitioner = sarama.NewHashPartitioner
	case "random":
		config.Producer.Partitioner = sarama.NewRandomPartitioner
	case "manual":
		config.Producer.Partitioner = sarama.NewManualPartitioner
		if setting["partition"] == "-1" {
			logger.Fatalln("Partition is required when partitioning manually.")
		}
	default:
		logger.Fatalf("Partitioner %s not supported.\n", setting["partitioner"])
	}
	// init partition
	i, err := strconv.Atoi(setting["partition"])
	if err != nil {
		logger.Fatalf("Failed to convert partition setting : %s to int.\n", setting["partition"])
	}
	partition = int32(i)
	// init topic
	topic = setting["topic"]
	config.Producer.RequiredAcks = sarama.WaitForAll
	// init producer
	producer, err = sarama.NewSyncProducer(strings.Split(setting["brokers"], ","), config)
	if err != nil {
		logger.Fatalln("Init failed:", err)
	}
	logger.Println("Init completed")
}

// SendByteMessage sends a byte slice message to kafka
func SendByteMessage(msg []byte) (partition int32, offset int64, err error) {
	message := &sarama.ProducerMessage{Topic: topic, Partition: partition}
	message.Value = sarama.ByteEncoder(msg)
	return producer.SendMessage(message)
}

// SendStringMessage sends a string message to kafka
func SendStringMessage(msg string) (partition int32, offset int64, err error) {
	message := &sarama.ProducerMessage{Topic: topic, Partition: partition}
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
