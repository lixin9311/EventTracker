package eventtracker

import (
	"github.com/naoina/toml"
	"log"
	"os"
)

type main_config struct {
	Http_listen_addr string
	Log_file         string
	Backup_file      string
}

type kafka_config struct {
	Brokers     []string
	Partitioner string
	Partition   int
	Topics      map[string]string
}

type avro_config struct {
	Schema string
}

type front_config struct {
	Enabled                  bool
	Service_reg_addr         string
	Backend_http_listen_addr string
}

type extension_config_anwo struct {
	Kafka_clk_topic      string
	Pid                  string
	Api_url              string
	Key                  string
	Td_postback_url      string
	Kafka_consumer_group string
	Zookeeper            string
}

type extension_config struct {
	Anwo extension_config_anwo
}

type Config struct {
	Main      main_config
	Kafka     kafka_config
	Avro      avro_config
	Front     front_config
	Extension extension_config
}

func ParseConfig(path string) *Config {
	file, err := os.Open(path)
	if err != nil {
		log.Fatalln("Failed to open config file:", err)
	}
	var conf Config
	decoder := toml.NewDecoder(file)
	err = decoder.Decode(&conf)
	if err != nil {
		log.Fatalln("Failed to parse config file:", err)
	}
	return &conf
}
