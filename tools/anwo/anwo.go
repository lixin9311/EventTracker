package main

import (
	"bytes"
	"database/sql"
	"flag"
	"fmt"
	_ "github.com/alexbrainman/odbc"
	"github.com/lixin9311/EventTracker/avro"
	"github.com/lixin9311/EventTracker/config"
	"github.com/lixin9311/EventTracker/kafka"
	"github.com/lixin9311/tablewriter"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"time"
)

const (
	DEBUG = false
)

var (
	configFile      = flag.String("c", "config.json", "config file.")
	conf            *config.Config
	lsadvertiser    = flag.Bool("ls", false, "list advertisers.")
	advertiser_id   = flag.Int64("id", -1, "advertiser id.")
	topic           = flag.String("t", "event", "topic to use.")
	pwd             = flag.String("p", "", "database password.")
	user            = flag.String("u", "", "database user.")
	connection      *sql.DB
	ad              = advertiser{}
	ad_groupid_list = ad_group_id_list{}
	logger          *log.Logger
	messages        = make(chan []byte, 128)
	closing         = make(chan struct{})
	transport       = http.Transport{MaxIdleConnsPerHost: 200}
	client          = &http.Client{Transport: &transport}
)

type ad_group_id_list struct {
	sync.Mutex
	data map[int64]struct{}
}

func (self *ad_group_id_list) size() int {
	return len(self.data)
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
}

func readKafka() {
	for {
		buffer := new(bytes.Buffer)
		buffer.Write(<-messages)
		record, err := avro.Decode(buffer)
		gid, err := record.Get("gid")
		if err != nil {
			logger.Fatalln("Failed to get ad_group_id")
			continue
		}
		log.Println(record)
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
		id, err := strconv.ParseInt(gid.(string), 10, 64)
		if err != nil {
			log.Printf("Failed to parse string(%s) to int: %s", gid.(string), err)
			continue
		}
		if !ad_groupid_list.get(id) {
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
		ext, err := record.Get("extension")
		if err != nil {
			logger.Println("Failed to get extension:", err)
			continue
		}
		ext_map := ext.(map[string]interface{})
		if _, ok := ext_map["agent"]; !ok {
			logger.Println("Not found agent in ext_mapension.")
			continue
		}
		if _, ok := ext_map["os_version"]; !ok {
			logger.Println("Not found os_version in ext_mapension.")
			continue
		}
		if _, ok := ext_map["device_model"]; !ok {
			logger.Println("Not found device_model in ext_mapension.")
			continue
		}
		keywords := ""
		base := "http://offer.adwo.com/offerwallcharge/clk"
		url := fmt.Sprintf("%s?advid=%s&ip=%s&cts=%s&osv=%s&mobile=%s&idfa=%s&keywords=%s", base, ext_map["agent"].(string), ip.(string), cts.(string), ext_map["os_version"].(string), ext_map["device_model"].(string), idfa.(string), keywords)
		log.Println(url)
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

func init() {
	flag.Parse()
	conf = config.ParseConfig(*configFile)
	logger = log.New(os.Stderr, "[main]:", log.LstdFlags|log.Lshortfile)
	file, err := os.OpenFile(conf.MainSetting["logfile"], os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		logger.Fatalln("Failed to open log file:", err)
	}
	w := io.MultiWriter(file, os.Stderr)
	logger.SetOutput(w)
	logger.Println("======== Loading Config Complete ========")
	avro.Init(w, conf.AvroSetting)
	kafka.Init(w, conf.KafkaSetting)
}

func main() {
	var err error
	if DEBUG {
		connection, err = sql.Open("odbc", "Servername=%s;Port=%s;Locale=en_US;Database=%s;UID=%s;PWD=%s;Driver=//opt//vertica//lib64//libverticaodbc.so;")
		defer connection.Close()
		if err != nil {
			logger.Fatalln("Failed to open:", err)
		}
		if *lsadvertiser {
			lsadvertisers()
			return
		}
		if *advertiser_id == -1 {
			logger.Println("Missing advertiser_id")
			flag.PrintDefaults()
			os.Exit(2)
		}
		row := connection.QueryRow("SELECT advertiser_id, display_name, loggeron_name FROM advertiser WHERE advertiser_id=?", *advertiser_id)
		err = row.Scan(ad.Id(), ad.Name(), ad.Logon())
		if err != nil {
			if err == sql.ErrNoRows {
				logger.Fatalln("No such user with that ID:", err)
			} else {
				logger.Fatalln("Failed to read Database:", err)
			}
		}
		go func() {
			updateList()
			time.Sleep(2 * time.Minute)
		}()

		go func() {
			if ad_groupid_list.size() > 100000 {
				ad_groupid_list.flush()
			}
			updateList()
			time.Sleep(2 * time.Minute)
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
	go kafka.Consumer(*topic, messages, closing)
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
