package main

import (
	"bytes"
	"crypto/md5"
	"database/sql"
	"flag"
	"fmt"
	_ "github.com/alexbrainman/odbc"
	"github.com/gorilla/mux"
	et "github.com/lixin9311/EventTracker/eventtracker"
	"github.com/lixin9311/tablewriter"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"time"
)

var (
	configFile      = flag.String("c", "config.json", "config file.")
	DEBUG           = flag.Bool("D", false, "Enable Database.")
	verbose         = flag.Bool("v", false, "Verbose")
	lsadvertiser    = flag.Bool("ls", false, "list advertisers.")
	conf            *et.Config
	connection      *sql.DB
	ad              = advertiser{}
	ad_groupid_list = ad_group_id_list{data: map[int64](struct{}){}}
	logger          *log.Logger
	messages        = make(chan []byte, 128)
	closing         = make(chan struct{})
	transport       = http.Transport{MaxIdleConnsPerHost: 200}
	client          = &http.Client{Transport: &transport}
	// Fail safe buffer file
	fail_safe *log.Logger
	address   []string
	kafka     *et.Kafka
	avro      *et.Avro
)

type ad_group_id_list struct {
	sync.Mutex
	data map[int64]struct{}
}

func (self *ad_group_id_list) size() int {
	return len(self.data)
}

func (self *ad_group_id_list) print() {
	logger.Println(self.data)
}

func (self *ad_group_id_list) flush() {
	self.Lock()
	defer self.Unlock()
	self.data = map[int64](struct{}){}
}

func (self *ad_group_id_list) put(id int64) {
	self.Lock()
	defer self.Unlock()
	self.data[id] = struct{}{}
}

func (self *ad_group_id_list) put_unsafe(id int64) {
	self.data[id] = struct{}{}
}

func (self *ad_group_id_list) get(id int64) bool {
	self.Lock()
	defer self.Unlock()
	if _, ok := self.data[id]; ok {
		return true
	}
	return false
}

type advertiser struct {
	advertiser_id int64
	display_name  string
	logon_name    string
}

func (self *advertiser) Id() *int64 {
	return &self.advertiser_id
}

func (self *advertiser) Name() *string {
	return &self.display_name
}

func (self *advertiser) Logon() *string {
	return &self.logon_name
}

func (self advertiser) String() string {
	return fmt.Sprintf("Id: %d, Name: %s, Logon name: %s.", self.advertiser_id, self.display_name, self.logon_name)
}

func (self advertiser) Array() []string {
	return []string{fmt.Sprintf("%d", self.advertiser_id), fmt.Sprintf("%s", self.display_name), fmt.Sprintf("%s", self.logon_name)}
}

func lsadvertisers() {
	rows, err := connection.Query("SELECT advertiser_id, display_name, Logon_name FROM advertiser;")
	defer rows.Close()
	if err != nil {
		logger.Fatal("Failed to query the Database:", err)
	}
	cols, err := rows.Columns()
	if err != nil {
		logger.Fatal("Failed to read columns from Database:", err)
	}
	table := tablewriter.NewWriter(os.Stdout)
	table.SetAlignment(tablewriter.ALIGN_RIGHT)
	table.SetHeader(cols)
	advertisers := make([]*advertiser, 0)
	for rows.Next() {
		ader := new(advertiser)
		if err := rows.Scan(ader.Id(), ader.Name(), ader.Logon()); err != nil {
			logger.Fatal("Failed to read Database:", err)
		}
		advertisers = append(advertisers, ader)
		table.Append(ader.Array())
	}
	if err := rows.Err(); err != nil {
		logger.Println("Error occured when reading database:", err)
	}
	table.Render()
	return
}

func updateList() {
	rows, err := connection.Query("SELECT ad_group_id FROM ad_group INNER JOIN campaign ON ad_group.campaign_id=campaign.campaign_id WHERE campaign.advertiser_id=?", *ad.Id())
	ad_group_id := int64(0)
	if err != nil {
		logger.Fatal("Failed to query Database:", err)
	}
	ad_groupid_list.Lock()
	defer ad_groupid_list.Unlock()
	for rows.Next() {
		rows.Scan(&ad_group_id)
		ad_groupid_list.put_unsafe(ad_group_id)
	}
	if err := rows.Err(); err != nil {
		logger.Println("Error occured when reading Database:", err)
	}
	if *verbose {
		logger.Println("ad_group_id_list updated!")
		ad_groupid_list.print()
	}
}

func readKafka() {
	for {
		buffer := new(bytes.Buffer)
		buffer.Write(<-messages)
		record, err := avro.Decode(buffer)
		event, err := record.Get("event")
		if err != nil {
			logger.Println("Failed to get event:", err)
		}
		if event.(string) != "click" {
			continue
		}
		gid, err := record.Get("gid")
		if err != nil {
			logger.Println("Failed to get ad_group_id:", err)
			continue
		}
		switch gid.(type) {
		case string:
			// got u
		case nil:
			logger.Println("gid doee not exist.")
			continue
		default:
			logger.Println("Unknow type of gid after avro decode:", gid)
			continue
		}
		if *DEBUG {

			id, err := strconv.ParseInt(gid.(string), 10, 64)
			if err != nil {
				log.Printf("Failed to parse string(%s) to int: %s", gid.(string), err)
				continue
			}
			if !ad_groupid_list.get(id) {
				//if false {
				logger.Println("The gid does not match!")
				continue
			}
		}
		if *verbose {
			logger.Println("gid matched!:", record)
		}
		aid, err := record.Get("aid")
		if err != nil {
			logger.Println("Failed to get aid:", err)
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
		ext, err := record.Get("extension")
		if err != nil {
			logger.Println("Failed to get extension:", err)
			continue
		}
		ext_map := ext.(map[string]interface{})
		if _, ok := ext_map["adv_id"]; !ok {
			logger.Println("Not found adv_id in ext_map.")
			continue
		}
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
		keywords := aid.(string)
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
	if fmt.Sprintf("%x", crypted) != r.Form["sign"][0] {
		logger.Printf("Sign not matched!: %x :%s\n", crypted, r.Form["sign"][0])
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
	record.Set("aid", r.Form["keyword"][0])

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
	record.Set("event", "TrackerEvent")
	record.Set("id", "")
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
	var err error
	if *DEBUG {
		db_str := fmt.Sprintf("Servername=%s;Port=%d;Locale=en_US;Database=%s;UID=%s;PWD=%s;Driver=//opt//vertica//lib64//libverticaodbc.so;", conf.Extension.Anwo.Db_server, conf.Extension.Anwo.Db_port, conf.Extension.Anwo.Db, conf.Extension.Anwo.Db_user, conf.Extension.Anwo.Db_pwd)
		connection, err = sql.Open("odbc", db_str)
		if err != nil {
			logger.Fatalln("Failed to open:", err)
		}
		if *lsadvertiser {
			lsadvertisers()
			return
		}
		defer connection.Close()
		row := connection.QueryRow("SELECT advertiser_id, display_name, logon_name FROM advertiser WHERE advertiser_id=?", conf.Extension.Anwo.Advertiser_id)
		err = row.Scan(ad.Id(), ad.Name(), ad.Logon())
		if err != nil {
			if err == sql.ErrNoRows {
				logger.Fatalln("No such user with that ID:", err)
			} else {
				logger.Fatalln("Failed to read Database:", err)
			}
		}
		go func() {
			for {
				if ad_groupid_list.size() > 100000 {
					ad_groupid_list.flush()
				}
				updateList()
				time.Sleep(2 * time.Minute)
			}
		}()
	}
	go func() {
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, os.Kill, os.Interrupt)
		<-signals
		logger.Println("Shutting down.")
		close(closing)
		time.Sleep(time.Second)
		os.Exit(0)
	}()
	go kafka.NewConsumer(conf.Extension.Anwo.Kafka_clk_topic, messages, closing)
	go serve_http()
	readKafka()
	//	var p person
	//	for rows.Next() {
	//		if err := rows.Scan(p.Id(), p.Name(), p.Age()); err != nil {
	//			logger.Fatalln("Failed to scan:", err)
	//		}
	//		logger.Println(p)
	//	}
	//	if err := rows.Err(); err != nil {
	//		logger.Fatalln(err)
	//	}
}
