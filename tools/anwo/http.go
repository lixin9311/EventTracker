package main

import (
	"bytes"
	"crypto/md5"
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
)

var (
	// MaxFileSize is the maximum size of upload file
	MaxFileSize = int64(10 * 1024 * 1024)
	// MaxMemorySize is the maximum memory size to handle the upload file
	MaxMemorySize = int64(10 * 1024 * 1024)
	brokers       = flag.String("brokers", "", "kafka brokers, this overrides config file.")
	partitioner   = flag.String("partitioner", "", "kafka partitioner, this overrides config file.")
	partition     = flag.String("partition", "", "kafka partition, this overrides config file.")
	schema        = flag.String("schema", "", "avro schema file, this overrides config file.")
	port          = flag.String("port", "", "http listen port, this overrides config file.")
	logfile       = flag.String("log", "", "logfile, this overrides config file.")
	bakfile       = flag.String("bakfile", "", "backup file, for fail safety when kafka write fails, this overrides config file.")
	// Fail safe buffer file
	fail_safe *log.Logger
	address   []string
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

// EventHandler is the REST api handler
func EventHandler(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	var remote string
	if tmp := r.Header.Get("X-Forwarded-For"); tmp != "" {
		remote = tmp
	} else {
		remote = r.RemoteAddr
	}
	logger.Println("Incomming event from:", remote)
	// required fields
	if len(r.Form["appid"]) < 1 {
		ErrorAndReturnCode(w, "Missing Required field: No appid", 400)
		return
	}
	if len(r.Form["adname"]) < 1 {
		ErrorAndReturnCode(w, "Missing Required field: No adname", 400)
		return
	}
	if len(r.Form["adid"]) < 1 {
		ErrorAndReturnCode(w, "Missing Required field: No adid", 400)
		return
	}
	if len(r.Form["device"]) < 1 {
		ErrorAndReturnCode(w, "Missing Required field: No device", 400)
		return
	}
	if len(r.Form["idfa"]) < 1 {
		ErrorAndReturnCode(w, "Missing Required field: No idfa", 400)
		return
	}
	if len(r.Form["point"]) < 1 {
		ErrorAndReturnCode(w, "Missing Required field: No point", 400)
		return
	}
	if len(r.Form["ts"]) < 1 {
		ErrorAndReturnCode(w, "Missing Required field: No ts", 400)
		return
	}
	if len(r.Form["sign"]) < 1 {
		ErrorAndReturnCode(w, "Missing Required field: No sign", 400)
		return
	}
	if len(r.Form["keywords"]) < 1 {
		ErrorAndReturnCode(w, "Missing Required field: No keywords", 400)
		return
	}
	if len(r.Form["key"]) < 1 {
		ErrorAndReturnCode(w, "Missing Required field: No key", 400)
		return
	}
	// set a new avro record
	str := fmt.Sprintf("adid=%sadname=%sappid=%sdevice=%sidfa=%spoint=%sts=%skey=%s", r.Form["adid"][0], r.Form["adname"][0], r.Form["appid"][0], r.Form["device"][0], r.Form["idfa"][0], r.Form["point"][0], r.Form["ts"][0], r.Form["key"][0])
	crypted := md5.Sum([]byte(str))
	if fmt.Sprintf("%x", crypted) != r.Form["sign"][0] {
		log.Printf("Sign not matched!: %x :%s\n", crypted, r.Form["sign"][0])
	}
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
	record.Set("did", r.Form["idfa"][0])
	record.Set("timestamp", r.Form["ts"][0])
	record.Set("event", "TrackerEvent")
	record.Set("id", "")
	// extensions fields
	extension := map[string](interface{}){}
	for k, v := range r.Form {
		if k != "ip" && k != "aid" && k != "idfa" && k != "timestamp" {
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
	part, offset, err := kafka.SendByteMessage(buf.Bytes(), "default")
	if err != nil {
		fail_safe.Println("error:", err)
		fail_safe.Println("record:", record)
		fail_safe.Println("data:", buf.Bytes())
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

func serve_http() {
	var err error
	// ~kafka
	defer kafka.Destroy()
	defer logger.Println("Instance down.")
	// REST route
	r := mux.NewRouter()
	r.HandleFunc("/", HomeHandler)
	r.HandleFunc("/anwo", EventHandler)
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
			address = []string{"/anwo", "http://" + ln.Addr().String()}
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
