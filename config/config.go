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
	MainSetting  map[string]string      `json:"main"`
	AvroSetting  map[string]string      `json:"avro"`
	FrontSetting map[string]interface{} `json:"front"`
	KafkaSetting map[string]interface{} `json:"kafka"`
}

func checkConfig(config *Config) {
	defer func() {
		if err := recover(); err != nil {
			logger.Fatalln("Config cannot pass check, please check its format:", err)
		}
	}()
	if config.MainSetting == nil {
		config.MainSetting = map[string]string{}
	}

	if config.AvroSetting == nil {
		config.AvroSetting = map[string]string{}
	}

	if config.KafkaSetting == nil {
		config.KafkaSetting = map[string](interface{}){}
	}

	if config.FrontSetting == nil {
		config.FrontSetting = map[string](interface{}){}
	}

	if _, ok := config.MainSetting["port"]; !ok {
		logger.Println("Missing main.port, using default value:", "8080")
		config.MainSetting["port"] = "8080"
	}

	if _, ok := config.FrontSetting["enable"]; !ok {
		logger.Println("Missing front.port, using default value:", "false")
		config.FrontSetting["enable"] = false
	}

	if _, ok := config.FrontSetting["address"]; !ok {
		logger.Println("Missing front.port, using default value:", "127.0.0.1:8081")
		config.FrontSetting["address"] = "127.0.0.1:8081"
	}

	if _, ok := config.FrontSetting["backend_http_listen_address"]; !ok {
		logger.Println("Missing front.backend_http_listen_address, using default value:", "127.0.0.1:0")
		config.FrontSetting["backend_http_listen_addres"] = "127.0.0.1:0"
	}

	if _, ok := config.MainSetting["bakfile"]; !ok {
		logger.Println("Missing main.bakfile, using default value:", "backup.log")
		config.MainSetting["bakfile"] = "backup.log"
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
		logger.Println("Missing kafka.topic, using default value:", map[string]string{"default": "default", "activation": "activation", "order": "order", "registration": "registration"})
		config.KafkaSetting["topic"] = map[string]string{"default": "default", "activation": "activation", "order": "order", "registration": "registration"}
	} else {
		for k, v := range config.KafkaSetting["topic"].(map[string]interface{}) {
			if k != "default" && k != "registration" && k != "order" && k != "activation" {
				logger.Printf("Unkown field in kafka.topic, k: %s, val: %s.", k, v)
			}
		}
		if _, ok := config.KafkaSetting["topic"].(map[string]interface{})["default"]; !ok {
			logger.Println("Missing kafka.topic.default, using default value:", "default")
			config.KafkaSetting["topic"].(map[string]interface{})["default"] = "default"
		}
		if _, ok := config.KafkaSetting["topic"].(map[string]interface{})["activation"]; !ok {
			logger.Println("Missing kafka.topic.activation, using default value:", "activation")
			config.KafkaSetting["topic"].(map[string]interface{})["activation"] = "activation"
		}
		if _, ok := config.KafkaSetting["topic"].(map[string]interface{})["order"]; !ok {
			logger.Println("Missing kafka.topic.order, using default value:", "order")
			config.KafkaSetting["topic"].(map[string]interface{})["order"] = "order"
		}
		if _, ok := config.KafkaSetting["topic"].(map[string]interface{})["registration"]; !ok {
			logger.Println("Missing kafka.topic.registration, using default value:", "registration")
			config.KafkaSetting["topic"].(map[string]interface{})["registration"] = "registration"
		}
	}

	if _, ok := config.KafkaSetting["partitioner"]; !ok {
		logger.Println("Missing kafka.partitioner, using default value:", "hash")
		config.KafkaSetting["partitioner"] = "hash"
	}

	if _, ok := config.KafkaSetting["partition"]; !ok {
		logger.Println("Missing kafka.partition, using default value:", "-1")
		config.KafkaSetting["partition"] = -1
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
