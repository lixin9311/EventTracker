package main

import (
	"flag"
	"github.com/gorilla/mux"
	et "github.com/lixin9311/EventTracker/eventtracker"
	"github.com/lixin9311/lfshook"
	"github.com/lixin9311/logrus"
	golog "log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

var (
	conf       *et.Config
	configFile = flag.String("c", "config.toml", "Config file in json.")
	// Fail safe buffer file
	address        []string
	defaultHandler *et.DefaultHandler
	kafka          *et.Kafka
	log            *logrus.Logger
)

func init() {
	// Parse flags
	flag.Parse()
	// open config
	conf = et.ParseConfig(*configFile)
	// Init log system
	log = logrus.New()
	log.Formatter = new(logrus.TextFormatter)
	// Init log to file
	switch conf.Main.Log_file_formatter {
	case "json":
		log.Hooks.Add(lfshook.NewHook(lfshook.PathMap{
			logrus.InfoLevel:  conf.Main.Log_file,
			logrus.ErrorLevel: conf.Main.Log_file,
			logrus.DebugLevel: conf.Main.Log_file,
			logrus.PanicLevel: conf.Main.Log_file,
			logrus.FatalLevel: conf.Main.Log_file,
			logrus.WarnLevel:  conf.Main.Log_file,
		}, new(logrus.JSONFormatter)))
	case "text":
		formatter := new(logrus.TextFormatter) // default
		formatter.ForceUnColored = true
		log.Hooks.Add(lfshook.NewHook(lfshook.PathMap{
			logrus.InfoLevel:  conf.Main.Log_file,
			logrus.ErrorLevel: conf.Main.Log_file,
			logrus.DebugLevel: conf.Main.Log_file,
			logrus.PanicLevel: conf.Main.Log_file,
			logrus.FatalLevel: conf.Main.Log_file,
			logrus.WarnLevel:  conf.Main.Log_file,
		}, formatter))
	default:
		log.Fatalln("Unrecognized log file formatter:", conf.Main.Log_file_formatter)
	}
	switch conf.Main.Log_level {
	case "debug":
		log.Level = logrus.DebugLevel
	case "info":
		log.Level = logrus.InfoLevel
	case "warn":
		log.Level = logrus.WarnLevel
	case "error":
		log.Level = logrus.ErrorLevel
	case "fatal":
		log.Level = logrus.FatalLevel
	case "panic":
		log.Level = logrus.PanicLevel
	default:
		log.Fatalln("Unrecognized log level:", conf.Main.Log_level)
	}
	// setup backup fi.e
	safe_file, err := os.OpenFile(conf.Main.Backup_file, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.WithFields(logrus.Fields{
			"module": "main",
		}).Fatalln("Failed to open backup file:", err)
	}
	fail_safe := golog.New(safe_file, "", golog.LstdFlags)
	// init avro
	avro := et.NewAvroInst(log, conf.Avro)
	// init kafka
	kafka = et.NewKafkaInst(log, conf.Kafka)
	defaultHandler = et.NewDefaultHandler(log, fail_safe, kafka, avro)
	log.WithFields(logrus.Fields{
		"module": "main",
	}).Infoln("Initialization done.")
}

func main() {
	var err error
	// ~kafka
	defer kafka.Destroy()
	defer log.WithFields(logrus.Fields{
		"module": "main",
	}).Infoln("Instance down.")
	// REST route
	r := mux.NewRouter()
	r.HandleFunc("/", defaultHandler.HomeHandler)
	r.HandleFunc("/event", defaultHandler.EventHandler)
	r.HandleFunc("/upload", defaultHandler.UploadHandler)
	r.HandleFunc("/ping", et.PingHandler)
	// bring up the service
	var ln net.Listener
	if conf.Front.Enabled == true {
		log.WithFields(logrus.Fields{
			"module": "main",
		}).Infoln("Front server Enabled.")
		ln, err = net.Listen("tcp", conf.Front.Backend_http_listen_addr)
		if err != nil {
			log.WithFields(logrus.Fields{
				"module": "main",
			}).Fatalln("Backend service failed to listen address[", conf.Front.Backend_http_listen_addr, "]:", err)
		}
		log.WithFields(logrus.Fields{
			"module": "main",
		}).Infoln("Http server is listening at:", ln.Addr())
		go func() {
			// reg service to front
			log.WithFields(logrus.Fields{
				"module": "main",
			}).Infoln("Registering service to front server:", conf.Front.Service_reg_addr)
			conn, err := net.Dial("tcp", conf.Front.Service_reg_addr)
			if err != nil {
				log.WithFields(logrus.Fields{
					"module": "main",
				}).Fatalln("Failed to connect to the front service:", err)
			}
			rpcClient := rpc.NewClient(conn)
			address = []string{"/", "http://" + ln.Addr().String()}
			var response error
			err = rpcClient.Call("Handle.Update", &address, &response)
			if err != nil {
				log.WithFields(logrus.Fields{
					"module": "main",
				}).Fatalln("Failed to register service:", err)
			}
			if response != nil {
				log.WithFields(logrus.Fields{
					"module": "main",
				}).Fatalln("Failed to register service:", response)
			}
			rpcClient.Close()
			log.WithFields(logrus.Fields{
				"module": "main",
			}).Infoln("Registered to the front service.")
		}()
		defer func() {
			// reg service to front
			log.WithFields(logrus.Fields{
				"module": "main",
			}).Infoln("Unsigning service from front server:", conf.Front.Service_reg_addr)
			conn, err := net.Dial("tcp", conf.Front.Service_reg_addr)
			if err != nil {
				log.WithFields(logrus.Fields{
					"module": "main",
				}).Fatalln("Failed to connect to the front service:", err)
			}
			rpcClient := rpc.NewClient(conn)
			var response error
			err = rpcClient.Call("Handle.Delete", &address, &response)
			if err != nil {
				log.WithFields(logrus.Fields{
					"module": "main",
				}).Infoln("Failed to unsign:", err)
			}
			if response != nil {
				log.WithFields(logrus.Fields{
					"module": "main",
				}).Infoln("Failed to unsign:", response)
			}
			rpcClient.Close()
			log.WithFields(logrus.Fields{
				"module": "main",
			}).Infoln("Gracefully unsigned from front serive.")
		}()
	} else {
		log.WithFields(logrus.Fields{
			"module": "main",
		}).Infoln("Front service not enabled.")
		ln, err = net.Listen("tcp", conf.Main.Http_listen_addr)
		if err != nil {
			log.WithFields(logrus.Fields{
				"module": "main",
			}).Fatalln("Fail to listen:", err)
		}
		log.WithFields(logrus.Fields{
			"module": "main",
		}).Infoln("Service listening at:", conf.Main.Http_listen_addr)
	}
	// err = http.ListenAndServe(":"+conf.MainSetting["port"], r)
	err = http.Serve(ln, r)
	if err != nil {
		log.WithFields(logrus.Fields{
			"module": "main",
		}).Fatalln("Failed to bring up http server:", err)
	}
}
