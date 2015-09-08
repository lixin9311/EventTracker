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
	"sync/atomic"
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
	balance    = flag.Bool("B", false, "Load Balance.")
	counter    = uint64(0)
)

type endpoint struct {
	*httputil.ReverseProxy
	url string
}

func newEndpoint(urlstr string) (*endpoint, error) {
	u, err := url.Parse(urlstr)
	if err != nil {
		return nil, err
	}
	return &endpoint{httputil.NewSingleHostReverseProxy(u), urlstr}, nil
}

type Handle struct {
	sync.Mutex
	url_str   string
	url       *url.URL
	endpoints map[string]*endpoint
	current   *endpoint
}

func (h *Handle) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	atomic.AddUint64(&counter, uint64(1))
	h.Lock()
	defer h.Unlock()
	if len(h.endpoints) == 0 {
		http.Error(w, "No server registrated.", 500)
		return
	}
	if *balance {
		goto BALANCE
	} else {
		if h.current != nil {
			if err := h.current.Ping(); err != nil {
				logger.Println("Current endpoint may not functioning, fallback:", err)
				goto FALLBACK
			}
			h.current.ServeHTTP(w, r)
		} else {
			http.Error(w, "No server registrated.", 500)
			return
		}
	}
	return

BALANCE:
	{
		for k, v := range h.endpoints {
			if err := v.Ping(); err != nil {
				delete(h.endpoints, k)
				continue
			}
			v.ServeHTTP(w, r)
			return
		}
		http.Error(w, "No server available.", 500)
		return
	}
FALLBACK:
	{
		for k, v := range h.endpoints {
			if err := v.Ping(); err != nil {
				delete(h.endpoints, k)
				continue
			}
			h.current = v
			v.ServeHTTP(w, r)
			return
		}
		http.Error(w, "No server available.", 500)
		return
	}
}

func (h *Handle) Update(req *string, rep *error) error {
	var err error
	h.Lock()
	defer h.Unlock()
	e, err := newEndpoint(*req)
	if err != nil {
		logger.Println("Failed to add a new endpoint:", err)
		*rep = err
		return err
	}
	if h.endpoints == nil {
		h.endpoints = map[string]*endpoint{*req: e}
	} else {
		h.endpoints[*req] = e
	}
	h.current = e
	logger.Println("Backend server address updated:", *req)
	*rep = nil
	return nil
}

func (h *Handle) Delete(req *string, rep *error) error {
	h.Lock()
	defer h.Unlock()
	logger.Println("Backend server unsigned:", *req)
	delete(h.endpoints, *req)
	*rep = nil
	return nil
}

func (h *Handle) Ping() {
	h.Lock()
	defer h.Unlock()
	for k, v := range h.endpoints {
		if err := v.Ping(); err != nil {
			delete(h.endpoints, k)
		}
	}
}

func (e *endpoint) Ping() error {
	resp, err := http.Get(e.url + "/ping")
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
			handle.Ping()
		}
	}()
	startService()
}
