package main

import (
	"flag"
	"github.com/lixin9311/EventTracker/config"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/rpc"
	"net/url"
	"os"
	"sync"
	"time"
)

var (
	handle     = new(Handle)
	logger     *log.Logger
	conf       *config.Config
	configFile = flag.String("c", "config.json", "Config file in json.")
	httpPort   = flag.String("http_port", "", "Http listen port.")
	rpcAddress = flag.String("rpc_address", "", "RPC service address.")
	logfile    = flag.String("log", "", "logfile.")
	force      = flag.Bool("F", false, "Force enable.")
)

type Handle struct {
	sync.Mutex
	url_str string
	url     *url.URL
}

func (h *Handle) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.Lock()
	defer h.Unlock()
	if h.url == nil {
		http.Error(w, "No server registrated.", 500)
		return
	}
	proxy := httputil.NewSingleHostReverseProxy(h.url)
	proxy.ServeHTTP(w, r)
	return
}

func (h *Handle) Update(req *string, rep *error) error {
	var err error
	h.Lock()
	defer h.Unlock()
	h.url_str = *req
	h.url, err = url.Parse(h.url_str)
	if err != nil {
		*rep = err
		logger.Println("Failed to update backend server address:", err)
		return err
	}
	logger.Println("Backend server address updated:", *req)
	*rep = nil
	return nil
}

func (h *Handle) Ping() error {
	h.Lock()
	if h.url_str == "" {
		h.Unlock()
		return nil
	}
	resp, err := http.Get(h.url_str + "/ping")
	h.Unlock()
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	io.Copy(ioutil.Discard, resp.Body)
	return nil
}

func startService() {
	logger.Println("Front server started, http service listening:", conf.MainSetting["port"])
	err := http.ListenAndServe(":"+conf.MainSetting["port"], handle)
	if err != nil {
		logger.Fatalln("Failed to listen:", err)
	}
}

func startRPC() {
	rpcServer := rpc.NewServer()
	rpcServer.Register(handle)
	l, err := net.Listen("tcp", conf.FrontSetting["address"].(string))
	if err != nil {
		logger.Fatalln("Failed to start RPC service:", err)
	}
	logger.Println("Front server started, reg service listening:", conf.FrontSetting["address"].(string))
	rpcServer.Accept(l)
}

func init() {
	flag.Parse()
	conf = config.ParseConfig(*configFile)
	logger = log.New(os.Stderr, "[front]:", log.LstdFlags|log.Lshortfile)
	if *logfile != "" {
		conf.MainSetting["logfile"] = *logfile
	}
	if *httpPort != "" {
		conf.MainSetting["port"] = *httpPort
	}
	if *rpcAddress != "" {
		conf.FrontSetting["address"] = *rpcAddress
	}
	if *force {
		conf.FrontSetting["enable"] = true
	}
	file, err := os.OpenFile(conf.MainSetting["logfile"], os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		logger.Fatalln("Failed to open log file:", err)
	}
	w := io.MultiWriter(file, os.Stderr)
	logger.SetOutput(w)
	if !conf.FrontSetting["enable"].(bool) {
		logger.Fatalln("Front service is not enabled, use -F to force.")
	}
}

func main() {
	go startRPC()
	go func() {
		for {
			time.Sleep(5 * time.Second)
			err := handle.Ping()
			if err != nil {
				logger.Println("Backend server may not be functioning:", err)
			}
		}
	}()
	startService()
}
