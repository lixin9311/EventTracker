package main

import (
	//	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/lixin9311/EventTracker/avro"
	"github.com/lixin9311/EventTracker/config"
	"github.com/lixin9311/EventTracker/kafka"
	"io/ioutil"
	"log"
	"os"
	"strings"
)

var (
	configFile = flag.String("c", "config.json", "config file")
	bakfile    = flag.String("i", "backup.log", "input backuofile")
	failed     = 0
	success    = 0
)

func readFromBackup() {
	file, err := os.Open(*bakfile)
	if err != nil {
		log.Println("Failed to open backup file:", err)
		return
	}
	filename := ""
	defer file.Close()
	for i := 0; i < 100; i++ {
		name := *bakfile + fmt.Sprintf(".%d", i)
		if _, err := os.Lstat(name); err != nil {
			filename = name
			break
		}
	}
	if filename == "" {
		log.Println("Failed to open another backup file. Too many of it.")
		return
	}
	buffer := new(bytes.Buffer)
	//rd := bufio.NewReader(file)
	content, err := ioutil.ReadAll(file)
	lines := strings.Split(string(content), "\n")
	for _, line := range lines {
		if !strings.Contains(line, "data:") {
			continue
		}
		newstr := strings.Split(line, "data:")
		if len(newstr) != 2 {
			log.Println("Read backup file, it should not hanppen.")
			continue
		}
		str := strings.Replace(newstr[1], " ", ",", -1)
		var data []byte
		json.Unmarshal([]byte(str), &data)
		buf := bytes.NewBuffer(data)
		record, err := avro.Decode(buf)
		if err != nil {
			log.Println("Failed to decode avro data:", err)
		}
		var etype string
		for _, field := range record.Fields {
			if field.Name == "ext" {
				etype = field.Datum.(map[string]interface{})["event_type"].(string)
				break
			}
		}
		_, _, err = kafka.SendByteMessage(data, etype)
		if err != nil {
			failed++
			log.Println("Failed to write kafka:", err)
			fmt.Fprintln(buffer, "data:"+newstr[1])
			continue
		}
		success++
	}
	if buffer.Len() != 0 {
		var file_bak *os.File
		file_bak, err = os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			log.Fatalln("Failed to open another backup file:", err)
		}
		log.Println("Writing to anothor file.")
		buffer.WriteTo(file_bak)
	}
}

func init() {
	flag.Parse()
	conf := config.ParseConfig(*configFile)
	kafka.Init(os.Stderr, conf.KafkaSetting)
	avro.Init(os.Stderr, conf.AvroSetting)
}

func main() {
	readFromBackup()
	log.Printf("Import complete: success: %d, failed: %d.\n", success, failed)
}
