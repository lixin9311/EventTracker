package main

import (
	"bytes"
	"encoding/csv"
	"flag"
	"fmt"
	"github.com/gorilla/mux"
	"github.com/lixin9311/EventTracker/avro"
	"github.com/lixin9311/EventTracker/config"
	"github.com/lixin9311/EventTracker/kafka"
	"html/template"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
)

var (
	// MaxFileSize is the maximum size of upload file
	MaxFileSize = int64(10 * 1024 * 1024)
	// MaxMemorySize is the maximum memory size to handle the upload file
	MaxMemorySize = int64(10 * 1024 * 1024)
	logger        *log.Logger
	conf          *config.Config
	configFile    = flag.String("c", "config.json", "Config file in json.")
	brokers       = flag.String("brokers", "", "kafka brokers, this overrides config file.")
	topic         = flag.String("topic", "", "kafka topic, this overrides config file.")
	partitioner   = flag.String("partitioner", "", "kafka partitioner, this overrides config file.")
	partition     = flag.String("partition", "", "kafka partition, this overrides config file.")
	schema        = flag.String("schema", "", "avro schema file, this overrides config file.")
	port          = flag.String("port", "", "http listen port, this overrides config file.")
	logfile       = flag.String("log", "", "logfile, this overrides config file.")
)

// HomeHandler is the index page for upload the csv file
func HomeHandler(w http.ResponseWriter, r *http.Request) {
	t, err := template.ParseFiles("index.html")
	if err != nil {
		logger.Fatalln("Parse index template failed:", err)
	}
	t.Execute(w, nil)
}

// ErrorAndReturnCode prints an error and reponse to http client
func ErrorAndReturnCode(w http.ResponseWriter, errstr string, code int) {
	logger.Println(errstr)
	http.Error(w, errstr, code)
}

// UploadHandler handles the upload file
func UploadHandler(w http.ResponseWriter, r *http.Request) {
	logger.Println("Incomming upload file from:", r.RemoteAddr)
	// limit the file size
	if r.ContentLength > MaxFileSize {
		ErrorAndReturnCode(w, "The file is too large:"+strconv.FormatInt(r.ContentLength, 10)+"bytes", 400)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, MaxFileSize)
	err := r.ParseMultipartForm(MaxMemorySize)
	MaxFileSize = 10 * 1024 * 1024
	if err != nil {
		ErrorAndReturnCode(w, "Failed to parse form:"+err.Error(), 500)
		return
	}
	// get the file
	file, _, err := r.FormFile("uploadfile")
	if err != nil {
		ErrorAndReturnCode(w, "Failed to read upload file:"+err.Error(), 500)
		return
	}
	defer file.Close()
	// read the file
	csvreader := csv.NewReader(file)
	record, err := csvreader.Read()
	if err != nil {
		ErrorAndReturnCode(w, "Failed to read the first line of file:"+err.Error(), 500)
		return
	}
	title := map[string]int{}
	ext := map[string]int{}
	extension := map[string](interface{}){}
	// read title and extensions
	for k, v := range record {
		if v == "did" || v == "aid" || v == "ip" || v == "timestamp" {
			title[v] = k
		} else {
			ext[v] = k
			logger.Println("csv file extension title:", v)
		}
	}
	// read the record one by one and send it to kafka
	buf := new(bytes.Buffer)
	counter := 0
	for {
		// one more line
		record, err := csvreader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			ErrorAndReturnCode(w, "Err read file:"+err.Error(), 500)
			return
		}
		arecord, err := avro.NewRecord()
		if err != nil {
			ErrorAndReturnCode(w, "Failed to set new avro record:"+err.Error(), 500)
			return
		}
		// set main title
		for k, v := range title {
			arecord.Set(k, record[v])
		}
		// set ext map
		for k, v := range ext {
			extension[k] = record[v]
		}
		if len(extension) != 0 {
			arecord.Set("extension", extension)
		}
		// fullfill the event.avsc required fields
		arecord.Set("event", "TrackerEvent")
		arecord.Set("id", "")
		// encode avro
		if err = avro.Encode(buf, arecord); err != nil {
			ErrorAndReturnCode(w, "Failed to encode avro record:"+err.Error(), 500)
			return
		}
		// send to kafka
		_, _, err = kafka.SendByteMessage(buf.Bytes())
		buf.Reset()
		if err != nil {
			ErrorAndReturnCode(w, "Failed to send to kafka:"+err.Error(), 500)
			return
		}
		counter++
	}
	// done
	logger.Printf("%d messages have been writen", counter)
	w.WriteHeader(200)
	fmt.Fprintf(w, "%d messages have been writen", counter)
}

// EventHandler is the REST api handler
func EventHandler(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	logger.Println("Incomming event from:", r.RemoteAddr)
	// required fields
	if len(r.Form["did"]) < 1 {
		ErrorAndReturnCode(w, "Missing Required field: No device id", 400)
		return
	}
	if len(r.Form["timestamp"]) < 1 {
		ErrorAndReturnCode(w, "Missing Required field: No timestamp", 400)
		return
	}
	// set a new avro record
	record, err := avro.NewRecord()
	if err != nil {
		ErrorAndReturnCode(w, "Failed to set a new avro record:"+err.Error(), 500)
		return
	}
	// optional fields
	if len(r.Form["ip"]) < 1 {
		logger.Println("no ip")
	} else {
		record.Set("ip", r.Form["ip"][0])
	}
	if len(r.Form["aid"]) < 1 {
		logger.Println("no aid")
	} else {
		record.Set("aid", r.Form["aid"][0])
	}
	// set required fields
	record.Set("did", r.Form["did"][0])
	record.Set("timestamp", r.Form["timestamp"][0])
	record.Set("event", "TrackerEvent")
	record.Set("id", "")
	// extensions fields
	extension := map[string](interface{}){}
	for k, v := range r.Form {
		if k != "ip" && k != "aid" && k != "did" && k != "timestamp" {
			extension[k] = v[0]
		}
	}
	if len(extension) != 0 {
		record.Set("extension", extension)
	}
	// encode avro
	buf := new(bytes.Buffer)
	if err = avro.Encode(buf, record); err != nil {
		ErrorAndReturnCode(w, "Failed to encode avro record:"+err.Error(), 500)
		return
	}
	// send to kafka
	part, offset, err := kafka.SendByteMessage(buf.Bytes())
	if err != nil {
		ErrorAndReturnCode(w, "Failed to send message to kafka:"+err.Error(), 500)
		return
	}
	// done
	logger.Printf("New record partition=%d\toffset=%d\n", part, offset)
	w.WriteHeader(200)
	fmt.Fprintf(w, "partition=%d&offset=%d", part, offset)
}

func init() {
	flag.Parse()
	// open config
	conf = config.ParseConfig(*configFile)
	logger = log.New(os.Stderr, "[main]:", log.LstdFlags|log.Lshortfile)
	// parse flags and override the config file
	if *brokers != "" {
		conf.KafkaSetting["brokers"] = *brokers
	}
	if *topic != "" {
		conf.KafkaSetting["topic"] = *topic
	}
	if *partitioner != "" {
		conf.KafkaSetting["partitioner"] = *partitioner
	}
	if *partition != "" {
		conf.KafkaSetting["partition"] = *partition
	}
	if *schema != "" {
		conf.AvroSetting["schema"] = *schema
	}
	if *logfile != "" {
		conf.MainSetting["logfile"] = *logfile
	}
	if *port != "" {
		conf.MainSetting["port"] = *port
	}
	file, err := os.OpenFile(conf.MainSetting["logfile"], os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		logger.Fatalln("Failed to open log file:", err)
	}
	// reset the logger
	w := io.MultiWriter(file, os.Stderr)
	logger.SetOutput(w)
	logger.Println("========== Loading Config Complete ==========")
	// init avro
	avro.Init(w, conf.AvroSetting)
	// init kafka
	kafka.Init(w, conf.KafkaSetting)
}

func main() {
	var err error
	// ~kafka
	defer kafka.Destroy()
	defer logger.Println("Instance down.")
	// REST route
	r := mux.NewRouter()
	r.HandleFunc("/", HomeHandler)
	r.HandleFunc("/event", EventHandler)
	r.HandleFunc("/upload", UploadHandler)
	// bring up the service
	logger.Println("Http server listening at 8080")
	err = http.ListenAndServe(":"+conf.MainSetting["port"], r)
	if err != nil {
		logger.Fatalln("Failed to listen http server:", err)
	}
}
