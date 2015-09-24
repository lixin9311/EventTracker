package main

import (
	"bytes"
	"crypto/md5"
	"flag"
	"fmt"
	"github.com/gorilla/mux"
	et "github.com/lixin9311/EventTracker/eventtracker"
	"github.com/lixin9311/lfshook"
	"github.com/lixin9311/logrus"
	"io"
	"io/ioutil"
	golog "log"
	"net"
	"net/http"
	"net/rpc"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"time"
)

var (
	configFile = flag.String("c", "config.toml", "config file.")
	sign_check = flag.Bool("f", false, "Force pass sign check.")
	log        *logrus.Logger
	conf       *et.Config
	transport  = http.Transport{MaxIdleConnsPerHost: 200}
	client     = &http.Client{Transport: &transport}
	// Fail safe buffer file
	fail_safe *golog.Logger
	address   []string
	kafka     *et.Kafka
	avro      *et.Avro
)

func readKafka() {
	log.WithFields(logrus.Fields{
		"module": "adwo",
	}).Debugln("Create consumer with consumer_group:%s, topics:%s, zookeepers:%s.", conf.Extension.Anwo.Kafka_consumer_group, conf.Extension.Anwo.Kafka_clk_topic, conf.Extension.Anwo.Zookeeper)
	consumer, err := kafka.NewConsumer(conf.Extension.Anwo.Kafka_consumer_group, strings.Split(conf.Extension.Anwo.Kafka_clk_topic, ","), conf.Extension.Anwo.Zookeeper)
	if err != nil {
		log.WithFields(logrus.Fields{
			"module": "adwo",
		}).Fatalln("Failed to create kafka consumer.")
	}
	defer consumer.Close()
	go func() {
		for err := range consumer.Errors() {
			log.WithFields(logrus.Fields{
				"module": "adwo",
			}).Errorln("Error occured when consume kafka:", err)
		}
	}()
	for message := range consumer.Messages() {
		buffer := new(bytes.Buffer)
		buffer.Write(message.Value)
		record, err := avro.Decode(buffer)
		event, err := record.Get("event")
		if err != nil {
			log.WithFields(logrus.Fields{
				"module": "adwo",
			}).Errorln("Failed to get event:", err)
			continue
		}
		if event.(string) != "click" {
			continue
		}
		ext, err := record.Get("extension")
		if err != nil {
			log.WithFields(logrus.Fields{
				"module": "adwo",
			}).Errorln("Failed to get extension:", err)
			continue
		}
		ext_map, ok := ext.(map[string]interface{})
		if !ok {
			log.WithFields(logrus.Fields{
				"module": "adwo",
			}).Errorln("Failed convert extesion to map.")
			continue
		}
		if adv_id, ok := ext_map["adv_id"]; !ok {
			//log.WithFields(logrus.Fields{
			//	"module": "adwo",
			//}).Debugln("Not found adv_id in ext_map. Maybe not an adwo ad.")
			continue
		} else if adv_id == nil || adv_id.(string) == "" {
			log.WithFields(logrus.Fields{
				"module": "adwo",
			}).Debugln("It should not happen.")
			log.WithFields(logrus.Fields{
				"module": "adwo",
			}).Debugln("The buggy record:", record)
			log.WithFields(logrus.Fields{
				"module": "adwo",
			}).Errorln("adv_id not valid, maybe not an adwo ad.")
			continue
		}
		id, err := record.Get("id")
		if err != nil {
			log.WithFields(logrus.Fields{
				"module": "adwo",
			}).Errorln("Failed to get auction_id:", err)
			continue
		}
		idfa, err := record.Get("did")
		if err != nil {
			log.WithFields(logrus.Fields{
				"module": "adwo",
			}).Errorln("Failed to get idfa:", err)
			continue
		}
		ip, err := record.Get("ip")
		if err != nil {
			log.WithFields(logrus.Fields{
				"module": "adwo",
			}).Errorln("Failed to get ip:", err)
			continue
		}
		cts, err := record.Get("timestamp")
		if err != nil {
			log.WithFields(logrus.Fields{
				"module": "adwo",
			}).Errorln("Failed to get timestamp:", err)
			continue
		}
		t, err := time.Parse(time.RFC3339, cts.(string))
		if err != nil {
			log.WithFields(logrus.Fields{
				"module": "adwo",
			}).Errorln("Failed to convert time:", err)
			continue
		}
		cts = fmt.Sprintf("%d", t.UTC().UnixNano()/1000000)
		if _, ok := ext_map["os_version"]; !ok {
			log.WithFields(logrus.Fields{
				"module": "adwo",
			}).Warnln("Not found os_version in ext_map.")
		}
		if _, ok := ext_map["device_model"]; !ok {
			log.WithFields(logrus.Fields{
				"module": "adwo",
			}).Warnln("Not found device_model in ext_map.")
			continue
		}
		for k, v := range ext_map {
			if _, ok := v.(string); !ok {
				log.WithFields(logrus.Fields{
					"module": "adwo",
				}).Warnf("%s unkown\n", k)
				ext_map[k] = "unknown"
			}
			if v.(string) == "" {
				log.WithFields(logrus.Fields{
					"module": "adwo",
				}).Warnf("%s unkown\n", k)
				ext_map[k] = "unknown"
			}
		}
		keywords := id.(string)
		pid := conf.Extension.Anwo.Pid
		base := conf.Extension.Anwo.Api_url
		mac := "AABBCCDDEEFF"
		log.WithFields(logrus.Fields{
			"module": "adwo",
		}).Debugln("Read AVRO record:", record)
		url := fmt.Sprintf("%s?pid=%s&advid=%s&ip=%s&cts=%s&osv=%s&mobile=%s&idfa=%s&mac=%s&keywords=%s", base, pid, ext_map["adv_id"].(string), ip.(string), cts.(string), ext_map["os_version"].(string), url.QueryEscape(ext_map["device_model"].(string)), idfa.(string), mac, keywords)
		log.WithFields(logrus.Fields{
			"module": "adwo",
		}).Debugln("Generated request url:", url)
		go func(url string) {
			request, err := http.NewRequest("GET", url, nil)
			if err != nil {
				log.WithFields(logrus.Fields{
					"module": "adwo",
				}).Errorln("Failed to create request:", err)
				return
			}
			request.Header.Add("Connection", "keep-alive")
			resp, err := client.Do(request)
			if err != nil {
				log.WithFields(logrus.Fields{
					"module": "adwo",
				}).Errorln("Failed to send clk to remote server:", err)
				return
			}
			if resp.StatusCode != 200 {
				log.WithFields(logrus.Fields{
					"module": "adwo",
				}).Errorln("Err when send clk:", resp.Status)
			}
			io.Copy(ioutil.Discard, resp.Body)
			resp.Body.Close()
		}(url)

	}

}

func ErrorAndReturnCode(w http.ResponseWriter, errstr string, code int) {
	log.WithFields(logrus.Fields{
		"module": "adwo",
	}).Errorln(errstr)
	http.Error(w, errstr, code)
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
	log.WithFields(logrus.Fields{
		"module": "adwo",
	}).Debugln("Incomming event from:", remote, "With Header:", r.Header)
	log.WithFields(logrus.Fields{
		"module": "adwo",
	}).Debugln("Request params:", r.Form)
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
	if len(r.Form["keyword"]) < 1 {
		ErrorAndReturnCode(w, "Missing Required field: No keyword", 400)
		return
	}
	// set a new avro record
	str := fmt.Sprintf("adid=%sadname=%sappid=%sdevice=%sidfa=%spoint=%sts=%skey=%s", r.Form["adid"][0], r.Form["adname"][0], r.Form["appid"][0], r.Form["device"][0], r.Form["idfa"][0], r.Form["point"][0], r.Form["ts"][0], conf.Extension.Anwo.Key)
	crypted := md5.Sum([]byte(str))
	if fmt.Sprintf("%x", crypted) != strings.Split(r.Form["sign"][0], ",")[0] {
		log.WithFields(logrus.Fields{
			"module": "adwo",
		}).Warnf("Sign not matched!: %x :%s\n, bypass sign check? %s", crypted, r.Form["sign"][0], *sign_check)
		if !*sign_check {
			ErrorAndReturnCode(w, "Sign mismatched!", 400)
			return
		}
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
	// set required fields
	record.Set("did", r.Form["idfa"][0])
	nsec, err := strconv.ParseInt(r.Form["ts"][0], 10, 64)
	if err != nil {
		log.WithFields(logrus.Fields{
			"module": "adwo",
		}).Errorln("Failed to parse ts to int:", err)
		ErrorAndReturnCode(w, "Failed to parse ts:"+err.Error(), 500)
		return
	}
	t := time.Unix(0, nsec*1000000)
	record.Set("timestamp", t.Format(time.RFC3339))
	record.Set("id", r.Form["keyword"][0])
	record.Set("event", "anwo_postback")
	record.Set("os", "ios")
	// extensions fields
	extension := map[string](interface{}){}
	for k, v := range r.Form {
		if k != "ip" && k != "aid" && k != "idfa" && k != "timestamp" && k != "keyword" && k != "sign" && k != "ts" {
			extension[k] = v[0]
		}
	}
	if len(extension) != 0 {
		record.Set("extension", extension)
	}
	log.WithFields(logrus.Fields{
		"module": "adwo",
	}).Debugln("Generated AVRO record:", record)
	// encode avro
	buf := new(bytes.Buffer)
	if err = avro.Encode(buf, record); err != nil {
		ErrorAndReturnCode(w, "Failed to encode avro record:"+err.Error(), 500)
		return
	}
	url := fmt.Sprintf("%s?params=%s", conf.Extension.Anwo.Td_postback_url, r.Form["keyword"][0])
	go func(url string) {
		log.WithFields(logrus.Fields{
			"module": "adwo",
		}).Debugln("Send postback to adserver with request url:", url)

		request, err := http.NewRequest("GET", url, nil)
		if err != nil {
			log.WithFields(logrus.Fields{
				"module": "adwo",
			}).Errorln("Failed to create request:", err)
			return
		}
		request.Header.Add("Connection", "keep-alive")
		resp, err := client.Do(request)
		if err != nil {
			log.WithFields(logrus.Fields{
				"module": "adwo",
			}).Errorln("Failed to send clk to remote server:", err)
			return
		}
		if resp.StatusCode != 200 {
			log.WithFields(logrus.Fields{
				"module": "adwo",
			}).Errorln("Error when send td_postback:", resp.Status)
			str, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				return
			}
			log.WithFields(logrus.Fields{
				"module": "adwo",
			}).Debugln("Resp body:", string(str))
			resp.Body.Close()
			return
		}
		io.Copy(ioutil.Discard, resp.Body)
		resp.Body.Close()
	}(url)

	// send to kafka
	part, offset, err := kafka.SendByteMessage(buf.Bytes(), "default")
	if err != nil {
		fail_safe.Println("error:", err)
		fail_safe.Println("record:", record)
		fail_safe.Println("data:", buf.Bytes())
		ErrorAndReturnCode(w, "Failed to send message to kafka:"+err.Error()+"Data has been writen to a backup file. Please contact us.", 200)
		return
	}
	// done
	log.WithFields(logrus.Fields{
		"module": "adwo",
	}).Debugf("New record partition=%d\toffset=%d\n", part, offset)
	w.WriteHeader(200)
	fmt.Fprintf(w, "1 messages have been writen.")
}

func serve_http() {
	var err error
	// ~kafka
	defer kafka.Destroy()
	defer log.WithFields(logrus.Fields{
		"module": "adwo",
	}).Infoln("Instance down.")
	// REST route
	r := mux.NewRouter()
	r.HandleFunc("/anwo", EventHandler)
	r.HandleFunc("/ping", et.PingHandler)
	// bring up the service
	var ln net.Listener
	if conf.Front.Enabled == true {
		log.WithFields(logrus.Fields{
			"module": "adwo",
		}).Infoln("Front server Enabled.")
		ln, err = net.Listen("tcp", conf.Front.Backend_http_listen_addr)
		if err != nil {
			log.WithFields(logrus.Fields{
				"module": "adwo",
			}).Fatalln("Backend service failed to listen address[", conf.Front.Backend_http_listen_addr, "]:", err)
		}
		log.WithFields(logrus.Fields{
			"module": "adwo",
		}).Infoln("Http server is listening at:", ln.Addr())
		go func() {
			// reg service to front
			log.WithFields(logrus.Fields{
				"module": "adwo",
			}).Infoln("Registering service to front server:", conf.Front.Service_reg_addr)
			conn, err := net.Dial("tcp", conf.Front.Service_reg_addr)
			if err != nil {
				log.WithFields(logrus.Fields{
					"module": "adwo",
				}).Fatalln("Failed to connect to the front service:", err)
			}
			rpcClient := rpc.NewClient(conn)
			address = []string{"/anwo", "http://" + ln.Addr().String()}
			var response error
			err = rpcClient.Call("Handle.Update", &address, &response)
			if err != nil {
				log.WithFields(logrus.Fields{
					"module": "adwo",
				}).Fatalln("Failed to register service:", err)
			}
			if response != nil {
				log.WithFields(logrus.Fields{
					"module": "adwo",
				}).Fatalln("Failed to register service:", response)
			}
			rpcClient.Close()
			log.WithFields(logrus.Fields{
				"module": "adwo",
			}).Infoln("Registered to the front service.")
		}()
		defer func() {
			// reg service to front
			log.WithFields(logrus.Fields{
				"module": "adwo",
			}).Infoln("Unsigning service from front server:", conf.Front.Service_reg_addr)
			conn, err := net.Dial("tcp", conf.Front.Service_reg_addr)
			if err != nil {
				log.WithFields(logrus.Fields{
					"module": "adwo",
				}).Fatalln("Failed to connect to the front service:", err)
			}
			rpcClient := rpc.NewClient(conn)
			var response error
			err = rpcClient.Call("Handle.Delete", &address, &response)
			if err != nil {
				log.WithFields(logrus.Fields{
					"module": "adwo",
				}).Infoln("Failed to unsign:", err)
			}
			if response != nil {
				log.WithFields(logrus.Fields{
					"module": "adwo",
				}).Infoln("Failed to unsign:", response)
			}
			rpcClient.Close()
			log.WithFields(logrus.Fields{
				"module": "adwo",
			}).Infoln("Gracefully unsigned from front serive.")
		}()
	} else {
		log.WithFields(logrus.Fields{
			"module": "adwo",
		}).Infoln("Front service not enabled.")
		ln, err = net.Listen("tcp", conf.Main.Http_listen_addr)
		if err != nil {
			log.WithFields(logrus.Fields{
				"module": "adwo",
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
			"module": "adwo",
		}).Fatalln("Failed to listen http server:", err)
	}
}

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
			"module": "adwo",
		}).Fatalln("Failed to open backup file:", err)
	}
	fail_safe = golog.New(safe_file, "", golog.LstdFlags)
	// init avro
	avro = et.NewAvroInst(log, conf.Avro)
	// init kafka
	kafka = et.NewKafkaInst(log, conf.Kafka)
	log.WithFields(logrus.Fields{
		"module": "adwo",
	}).Infoln("Initialization done.")
}

func main() {
	go func() {
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, os.Kill, os.Interrupt)
		<-signals
		log.WithFields(logrus.Fields{
			"module": "adwo",
		}).Infoln("Instance shutting down.")
		time.Sleep(time.Second)
		os.Exit(0)
	}()
	go serve_http()
	readKafka()
}
