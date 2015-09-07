package kafka

import (
	"github.com/Shopify/sarama"
	"io"
	"log"
	"strings"
)

var (
	producer   sarama.SyncProducer
	topic      = map[string]string{}
	partition  = int32(-1)
	brokerlist = "localhost:9092"
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
