package main

import (
	"bytes"
	"github.com/Shopify/sarama"
	"github.com/lixin9311/EventTracker/avro"
	"log"
	"strings"
	"time"
)

var (
	brokerList = "localhost:9092"
	topic      = "test"
	partition  = -1
)

func main() {
	err := avro.LoadSchema("./TrackerEvent.avsc")
	if err != nil {
		log.Fatalln("err load schema:", err)
	}
	record, err := avro.NewRecord()
	if err != nil {
		log.Fatalln(err)
	}
	record.Set("device_id", "device_id is here")
	record.Set("app_id", "app_id is here")
	record.Set("ip", "ip is here")
	record.Set("timestamp", time.Now().UTC().Unix())
	log.Println(record)
	buf := new(bytes.Buffer)
	if err = avro.Encode(buf, record); err != nil {
		log.Fatalln(err)
	}
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Partitioner = sarama.NewHashPartitioner
	message := &sarama.ProducerMessage{Topic: topic, Partition: int32(partition)}
	message.Value = sarama.ByteEncoder(buf.Bytes())
	producer, err := sarama.NewSyncProducer(strings.Split(brokerList, ","), config)
	if err != nil {
		log.Fatalln("err new producer:", err)
	}
	defer func() {
		if err := producer.Close(); err != nil {
			log.Println("Failed to close kafka producer:", err)
		}
	}()
	partition, offset, err := producer.SendMessage(message)
	if err != nil {
		log.Fatalln("err send message:", err)
	}
	log.Printf("topic=%s\tpartition=%d\toffset=%d\n", topic, partition, offset)
}
