package main

import (
	"flag"
	"github.com/gorilla/mux"
	et "github.com/lixin9311/EventTracker/eventtracker"
	"io"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

var (
	logger     *log.Logger
	conf       *et.Config
	configFile = flag.String("c", "config.json", "Config file in json.")
	// Fail safe buffer file
	address        []string
	defaultHandler *et.DefaultHandler
	kafka          *et.Kafka
)

func init() {
	flag.Parse()
	// open config
	conf = et.ParseConfig(*configFile)
	logger = log.New(os.Stderr, "[main]:", log.LstdFlags|log.Lshortfile)
	// parVse flags and override the config file
	safe_file, err := os.OpenFile(conf.Main.Backup_file, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		logger.Fatalln("Failed to open backup file:", err)
	}
	fail_safe := log.New(safe_file, "", log.LstdFlags)
	file, err := os.OpenFile(conf.Main.Log_file, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		logger.Fatalln("Failed to open log file:", err)
	}
	// reset the logger
	w := io.MultiWriter(file, os.Stderr)
	logger.SetOutput(w)
	logger.Println("========== Loading Config Complete ==========")
	// init avro
	avro := et.NewAvroInst(w, conf.Avro)
	// init kafka
	kafka = et.NewKafkaInst(w, conf.Kafka)
	defaultHandler = et.NewDefaultHandler(w, fail_safe, kafka, avro)
}

func main() {
	var err error
	// ~kafka
	defer kafka.Destroy()
	defer logger.Println("Instance down.")
	// REST route
	r := mux.NewRouter()
	r.HandleFunc("/", defaultHandler.HomeHandler)
	r.HandleFunc("/event", defaultHandler.EventHandler)
	r.HandleFunc("/upload", defaultHandler.UploadHandler)
	r.HandleFunc("/ping", et.PingHandler)
	// bring up the service
	var ln net.Listener
	if conf.Front.Enabled == true {
		logger.Println("Using a Front server.")
		ln, err = net.Listen("tcp", conf.Front.Backend_http_listen_addr)
		if err != nil {
			logger.Fatalln("Failed to listen:", err)
		}
		logger.Println("Http server listening a random local port at:", ln.Addr())
		go func() {
			// reg service to front
			logger.Println("Registering service to front server:", conf.Front.Service_reg_addr)
			conn, err := net.Dial("tcp", conf.Front.Service_reg_addr)
			if err != nil {
				logger.Fatalln("Failed to connect to the front service:", err)
			}
			rpcClient := rpc.NewClient(conn)
			address = []string{"/", "http://" + ln.Addr().String()}
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
			logger.Println("Unsigning service to front server:", conf.Front.Service_reg_addr)
			conn, err := net.Dial("tcp", conf.Front.Service_reg_addr)
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
		ln, err = net.Listen("tcp", conf.Main.Http_listen_addr)
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
