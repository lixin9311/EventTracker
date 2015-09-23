package main

import (
	"bytes"
	"crypto/md5"
	"flag"
	"fmt"
	"github.com/gorilla/mux"
	et "github.com/lixin9311/EventTracker/eventtracker"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"time"
)

var (
	configFile   = flag.String("c", "config.json", "config file.")
	verbose      = flag.Bool("v", false, "Verbose")
	sign_check   = flag.Bool("f", false, "Force pass sign check.")
	lsadvertiser = flag.Bool("ls", false, "list advertisers.")
	conf         *et.Config
	logger       *log.Logger
	messages     = make(chan []byte, 128)
	closing      = make(chan struct{})
	transport    = http.Transport{MaxIdleConnsPerHost: 200}
	client       = &http.Client{Transport: &transport}
	// Fail safe buffer file
	fail_safe *log.Logger
	address   []string
	kafka     *et.Kafka
	avro      *et.Avro
)

func readKafka() {
	consumer, err := kafka.NewConsumer(conf.Extension.Anwo.Kafka_consumer_group, strings.Split(conf.Extension.Anwo.Kafka_clk_topic, ","), conf.Extension.Anwo.Zookeeper)
	if err != nil {
		logger.Fatalln("Failed to create kafka consumer.")
	}
	defer consumer.Close()
	go func() {
		for err := range consumer.Errors() {
			logger.Println("Error occured when consume kafka:", err)
		}
	}()
	for message := range consumer.Messages() {
		buffer := new(bytes.Buffer)
		buffer.Write(message.Value)
		record, err := avro.Decode(buffer)
		event, err := record.Get("event")
		if err != nil {
			logger.Println("Failed to get event:", err)
		}
		if event.(string) != "click" {
			continue
		}
		ext, err := record.Get("extension")
		if err != nil {
			logger.Println("Failed to get extension:", err)
			continue
		}
		ext_map, ok := ext.(map[string]interface{})
		if !ok {
			logger.Println("Failed convert extesion to map.")
			continue
		}
		if _, ok := ext_map["adv_id"]; !ok {
			logger.Println("Not found adv_id in ext_map. Maybe not an adwo ad.")
			continue
		}
		id, err := record.Get("id")
		if err != nil {
			logger.Println("Failed to get auction_id:", err)
			continue
		}
		idfa, err := record.Get("did")
		if err != nil {
			logger.Println("Failed to get idfa:", err)
			continue
		}
		ip, err := record.Get("ip")
		if err != nil {
			logger.Println("Failed to get ip:", err)
			continue
		}

		cts, err := record.Get("timestamp")
		if err != nil {
			logger.Println("Failed to get timestamp:", err)
			continue
		}
		t, err := time.Parse(time.RFC3339, cts.(string))
		if err != nil {
			logger.Println("Failed to parse time from kafka:", err)
			continue
		}
		cts = fmt.Sprintf("%d", t.UTC().UnixNano()/1000000)
		if _, ok := ext_map["os_version"]; !ok {
			logger.Println("Not found os_version in ext_map.")
			continue
		}
		if _, ok := ext_map["device_model"]; !ok {
			logger.Println("Not found device_model in ext_map.")
			continue
		}
		for k, v := range ext_map {
			if _, ok := v.(string); !ok {
				logger.Printf("%s unkown\n", k)
				ext_map[k] = "unknown"
			}
			if v.(string) == "" {
				ext_map[k] = "unknown"
			}
		}
		keywords := id.(string)
		pid := conf.Extension.Anwo.Pid
		base := conf.Extension.Anwo.Api_url
		mac := "AABBCCDDEEFF"
		url := fmt.Sprintf("%s?pid=%s&advid=%s&ip=%s&cts=%s&osv=%s&mobile=%s&idfa=%s&mac=%s&keywords=%s", base, pid, ext_map["adv_id"].(string), ip.(string), cts.(string), ext_map["os_version"].(string), ext_map["device_model"].(string), idfa.(string), mac, keywords)
		logger.Println(url)
		go func(url string) {
			request, err := http.NewRequest("GET", url, nil)
			if err != nil {
				logger.Println("Failed to create request:", err)
				return
			}
			request.Header.Add("Connection", "keep-alive")
			resp, err := client.Do(request)
			if err != nil {
				logger.Println("Failed to send clk to remote server:", err)
				return
			}
			io.Copy(ioutil.Discard, resp.Body)
			resp.Body.Close()
		}(url)

	}

}

func ErrorAndReturnCode(w http.ResponseWriter, errstr string, code int) {
	logger.Println(errstr)
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
	logger.Println("Incomming event from:", remote)
	if *verbose {
		logger.Println("Request params:")
		logger.Println(r.Form)
	}
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
		logger.Printf("Sign not matched!: %x :%s\n", crypted, r.Form["sign"][0])
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
		logger.Println("Failed to parse ts to int:", err)
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
	if *verbose {
		logger.Println("Record to write:")
		logger.Println(record)
	}
	// encode avro
	buf := new(bytes.Buffer)
	if err = avro.Encode(buf, record); err != nil {
		logger.Println("AVRO record:", record)
		ErrorAndReturnCode(w, "Failed to encode avro record:"+err.Error(), 500)
		return
	}
	url := fmt.Sprintf("%s?params=%s", conf.Extension.Anwo.Td_postback_url, r.Form["keyword"][0])
	go func(url string) {
		request, err := http.NewRequest("GET", url, nil)
		if err != nil {
			logger.Println("Failed to create request:", err)
			return
		}
		request.Header.Add("Connection", "keep-alive")
		resp, err := client.Do(request)
		if err != nil {
			logger.Println("Failed to send clk to remote server:", err)
			return
		}
		if resp.StatusCode != 200 {
			logger.Println("Error when send td_postback:", resp.Status)
			str, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				return
			}
			logger.Println("Resp body:", string(str))
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
	logger.Printf("New record partition=%d\toffset=%d\n", part, offset)
	w.WriteHeader(200)
	fmt.Fprintf(w, "1 messages have been writen.")
}

func serve_http() {
	var err error
	// ~kafka
	defer kafka.Destroy()
	defer logger.Println("Instance down.")
	// REST route
	r := mux.NewRouter()
	r.HandleFunc("/anwo", EventHandler)
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

func init() {
	flag.Parse()
	var err error
	conf = et.ParseConfig(*configFile)
	logger = log.New(os.Stderr, "[main]:", log.LstdFlags|log.Lshortfile)
	file, err := os.OpenFile(conf.Main.Log_file, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		logger.Fatalln("Failed to open log file:", err)
	}
	safe_file, err := os.OpenFile(conf.Main.Backup_file, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		logger.Fatalln("Failed to open backup file:", err)
	}
	fail_safe = log.New(safe_file, "", log.LstdFlags)
	w := io.MultiWriter(file, os.Stderr)
	logger.SetOutput(w)
	logger.Println("======== Loading Config Complete ========")
	avro = et.NewAvroInst(w, conf.Avro)
	kafka = et.NewKafkaInst(w, conf.Kafka)
}

func main() {
	go func() {
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, os.Kill, os.Interrupt)
		<-signals
		logger.Println("Shutting down.")
		close(closing)
		time.Sleep(time.Second)
		os.Exit(0)
	}()
	go serve_http()
	readKafka()
}
