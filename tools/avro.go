package main

import (
	"bytes"
	"flag"
	et "github.com/lixin9311/EventTracker/eventtracker"
	"github.com/lixin9311/logrus"
)

var (
	configFile = flag.String("c", "config.toml", "config file.")
)

func main() {
	flag.Parse()
	log := logrus.New()
	conf := et.ParseConfig(*configFile)
	avro := et.NewAvroInst(log, conf.Avro)

	record, err := avro.NewRecord()
	if err != nil {
		log.Fatalln("New record err:", err)
	}
	record.Set("id", "auction_id")
	record.Set("event", "anwo_postback")
	record.Set("timestamp", "time")
	buf := new(bytes.Buffer)
	if err = avro.Encode(buf, record); err != nil {
		log.Fatalln("Encode err:", err)
	}
	decode, err := avro.Decode(buf)
	if err != nil {
		log.Fatalln("Err decode:", err)
	}
	log.Println(decode)
}
