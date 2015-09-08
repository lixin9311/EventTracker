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
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
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
	topic         = flag.String("topic", "", "kafka topics, the order is default, activation, registration, order, split with comma, this overrides config file.")
	partitioner   = flag.String("partitioner", "", "kafka partitioner, this overrides config file.")
	partition     = flag.String("partition", "", "kafka partition, this overrides config file.")
	schema        = flag.String("schema", "", "avro schema file, this overrides config file.")
	port          = flag.String("port", "", "http listen port, this overrides config file.")
	logfile       = flag.String("log", "", "logfile, this overrides config file.")
	bakfile       = flag.String("bakfile", "", "backup file, for fail safety, this overrides config file.")
	// Fail safe buffer file
	fail_safe *log.Logger
	address   string
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

func PingHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Pong")
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
	if _, ok := title["did"]; !ok {
		ErrorAndReturnCode(w, "Missing Required field: No did", 400)
		return
	}
	if _, ok := title["timestamp"]; !ok {
		ErrorAndReturnCode(w, "Missing Required field: No timestamp", 400)
		return
	}
	if _, ok := ext["event_type"]; !ok {
		ErrorAndReturnCode(w, "Missing Required field: No event_type", 400)
		return
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
		_, _, err = kafka.SendByteMessage(buf.Bytes(), record[ext["event_type"]])
		if err != nil {
			fail_safe.Println(err)
			fail_safe.Println(arecord)
			fail_safe.Println(buf.Bytes())
			ErrorAndReturnCode(w, "Failed to send to kafka:"+err.Error(), 500)
			return
		}
		buf.Reset()
		counter++
	}
	// done
	logger.Printf("%d messages have been writen.", counter)
	w.WriteHeader(200)
	fmt.Fprintf(w, "%d messages have been writen.", counter)
}

// EventHandler is the REST api handler
func EventHandler(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	logger.Println("Incomming event from:", r.RemoteAddr)
	// required fields
	if len(r.Form["did"]) < 1 {
		ErrorAndReturnCode(w, "Missing Required field: No did", 400)
		return
	}
	if len(r.Form["timestamp"]) < 1 {
		ErrorAndReturnCode(w, "Missing Required field: No timestamp", 400)
		return
	}
	if len(r.Form["event_type"]) < 1 {
		ErrorAndReturnCode(w, "Missing Required field: No event_type", 400)
		return
	}
	// set a new avro record
	record, err := avro.NewRecord()
	if err != nil {
		ErrorAndReturnCode(w, "Failed to set a new avro record:"+err.Error(), 500)
		return
	}
	// optional fields
	if len(r.Form["ip"]) > 0 {
		record.Set("ip", r.Form["ip"][0])
	}
	if len(r.Form["aid"]) > 0 {
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
		logger.Println("AVRO record:", record)
		ErrorAndReturnCode(w, "Failed to encode avro record:"+err.Error(), 500)
		return
	}
	// send to kafka
	part, offset, err := kafka.SendByteMessage(buf.Bytes(), r.Form["event_type"][0])
	if err != nil {
		fail_safe.Println(err)
		fail_safe.Println(record)
		fail_safe.Println(buf.Bytes())
		ErrorAndReturnCode(w, "Failed to send message to kafka:"+err.Error()+"Data has been writen to a backup file. Please contact us.", 500)
		return
	}
	// done
	logger.Printf("New record partition=%d\toffset=%d\n", part, offset)
	w.WriteHeader(200)
	fmt.Fprintf(w, "1 messages have been writen.")
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
		topic_data := map[string](interface{}){}
		topic_strs := strings.Split(*topic, ",")
		if len(topic_strs) != 4 {
			logger.Fatalln("Failed to parse flags: Not enough topics:", *topic)
		}
		topic_data["default"] = topic_strs[0]
		topic_data["activation"] = topic_strs[1]
		topic_data["registration"] = topic_strs[2]
		topic_data["order"] = topic_strs[3]
		conf.KafkaSetting["topic"] = topic_data
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
	if *bakfile != "" {
		conf.MainSetting["bakfile"] = *bakfile
	}
	safe_file, err := os.OpenFile(conf.MainSetting["bakfile"], os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		logger.Fatalln("Failed to open backup file:", err)
	}
	fail_safe = log.New(safe_file, "", log.LstdFlags)
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
	r.HandleFunc("/ping", PingHandler)
	// bring up the service
	var ln net.Listener
	if conf.FrontSetting["enable"].(bool) == true {
		logger.Println("Using a Front server.")
		ln, err = net.Listen("tcp", conf.FrontSetting["backend_http_listen_address"].(string))
		if err != nil {
			logger.Fatalln("Failed to listen:", err)
		}
		logger.Println("Http server listening a random local port at:", ln.Addr())
		go func() {
			// reg service to front
			logger.Println("Registering service to front server:", conf.FrontSetting["address"].(string))
			conn, err := net.Dial("tcp", conf.FrontSetting["address"].(string))
			if err != nil {
				logger.Fatalln("Failed to connect to the front service:", err)
			}
			rpcClient := rpc.NewClient(conn)
			address = "http://" + ln.Addr().String()
			var response error
			err = rpcClient.Call("Handle.Update", &address, &response)
			if err != nil {
				logger.Fatalln("Failed to register service:", err)
			}
			if response != nil {
				log.Fatalln("Failed to register service:", response)
			}
			rpcClient.Close()
			logger.Println("Registered to the front service.")
		}()
		defer func() {
			// reg service to front
			logger.Println("Unsigning service to front server:", conf.FrontSetting["address"].(string))
			conn, err := net.Dial("tcp", conf.FrontSetting["address"].(string))
			if err != nil {
				logger.Fatalln("Failed to connect to the front service:", err)
			}
			rpcClient := rpc.NewClient(conn)
			var response error
			err = rpcClient.Call("Handle.Delete", &address, &response)
			if err != nil {
				logger.Println("Failed to unsign:", err)
			}
			if response != nil {
				log.Println("Failed to unsign:", response)
			}
			rpcClient.Close()
			logger.Println("Gracefully unsigned from front serive.")
		}()
	} else {
		ln, err = net.Listen("tcp", ":"+conf.MainSetting["port"])
		if err != nil {
			logger.Fatalln("Fail to listen:", err)
		}
	}
	// err = http.ListenAndServe(":"+conf.MainSetting["port"], r)
	err = http.Serve(ln, r)
	if err != nil {
		logger.Fatalln("Failed to listen http server:", err)
	}
}
