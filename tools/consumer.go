package main

import (
	"bytes"
	"github.com/Shopify/sarama"
	"github.com/lixin9311/EventTracker/avro"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
)

var (
	brokerList = "localhost:9092"
	topic      = "test"
	offset     = sarama.OffsetOldest
	bufferSize = 256
	buffer     = new(bytes.Buffer)
)

func main() {
	err := avro.LoadSchema("./event.avsc")
	if err != nil {
		log.Fatalln("err load schema:", err)
	}
	consumer, err := sarama.NewConsumer(strings.Split(brokerList, ","), nil)
	if err != nil {
		log.Fatalln("err new consumer:", err)
	}
	partitionList, err := consumer.Partitions(topic)
	if err != nil {
		log.Fatalln("err get partition list:", err)
	}
	var (
		messages = make(chan *sarama.ConsumerMessage, bufferSize)
		closing  = make(chan struct{})
		wg       sync.WaitGroup
	)
	go func() {
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, os.Kill, os.Interrupt)
		<-signals
		log.Println("shutting down consumers")
		close(closing)
	}()

	for _, partition := range partitionList {
		pc, err := consumer.ConsumePartition(topic, partition, offset)
		if err != nil {
			log.Fatalln("err start to consume partition:", partition, ":", err)
		}
		go func(pc sarama.PartitionConsumer) {
			<-closing
			pc.AsyncClose()
		}(pc)
		wg.Add(1)
		go func(pc sarama.PartitionConsumer) {
			defer wg.Done()
			for message := range pc.Messages() {
				messages <- message
			}
		}(pc)
	}
	go func() {
		for msg := range messages {
			_, err := buffer.Write(msg.Value)
			if err != nil {
				log.Fatalln("err write to buffer:", err)
			}
			record, err := avro.Decode(buffer)
			if err != nil {
				log.Fatalln("err decode buffer:", err)
			}
			log.Println(record)
		}
	}()
	wg.Wait()
	log.Println("Done consuming topic:", topic)
	close(messages)
	if err := consumer.Close(); err != nil {
		log.Println("err closing consumer:", err)
	}

}
