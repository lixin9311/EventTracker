package config

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"os"
)

var (
	logger *log.Logger
)

func init() {
	logger = log.New(os.Stderr, "[config]:", log.LstdFlags|log.Lshortfile)
}

// Config is the basic struct of a config file
type Config struct {
	MainSetting  map[string]string `json:"main"`
	AvroSetting  map[string]string `json:"avro"`
	KafkaSetting map[string]string `json:"kafka"`
}

func checkConfig(config *Config) {
	if config.MainSetting == nil {
		config.MainSetting = map[string]string{}
	}

	if config.AvroSetting == nil {
		config.AvroSetting = map[string]string{}
	}

	if config.KafkaSetting == nil {
		config.KafkaSetting = map[string]string{}
	}

	if _, ok := config.MainSetting["port"]; !ok {
		logger.Println("Missing main.port, using default value:", "8080")
		config.MainSetting["port"] = "8080"
	}

	if _, ok := config.MainSetting["logfile"]; !ok {
		logger.Println("Missing main.logfile, using default value:", "tracker.log")
		config.MainSetting["logfile"] = "tracker.log"
	}

	if _, ok := config.KafkaSetting["brokers"]; !ok {
		logger.Println("Missing kafka.brokers, using default value:", "localhost:9092")
		config.KafkaSetting["brokers"] = "localhost:9092"
	}

	if _, ok := config.KafkaSetting["topic"]; !ok {
		logger.Println("Missing kafka.topic, using default value:", "test")
		config.KafkaSetting["topic"] = "test"
	}

	if _, ok := config.KafkaSetting["partitioner"]; !ok {
		logger.Println("Missing kafka.partitioner, using default value:", "hash")
		config.KafkaSetting["partitioner"] = "hash"
	}

	if _, ok := config.KafkaSetting["partition"]; !ok {
		logger.Println("Missing kafka.partition, using default value:", "-1")
		config.KafkaSetting["partition"] = "-1"
	}

	if _, ok := config.AvroSetting["schema"]; !ok {
		logger.Println("Missing avro.schema, using default value:", "event.avsc")
		config.AvroSetting["schema"] = "event.avsc"
	}
}

// ParseConfig construct a config from a JSON config file
func ParseConfig(path string) (config *Config) {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		logger.Fatalln("failed to read config file:", err)
		return nil
	}
	config = new(Config)
	if err = json.Unmarshal(data, config); err != nil {
		logger.Fatalln("failed to parse config file:", err)
		return nil
	}
	checkConfig(config)
	return
}
