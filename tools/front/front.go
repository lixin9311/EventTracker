package main

import (
	"errors"
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
	"strings"
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
	rpcAddress = flag.String("rpc_address", "", "RPC service registration address.")
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

func (e *endpoint) Ping() error {
	resp, err := http.Get(e.url + "/ping")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	io.Copy(ioutil.Discard, resp.Body)
	return nil
}

type endpoint_cluster struct {
	endpoints map[string]*endpoint
	prefix    string
	sync.Mutex
	current *endpoint
}

func newCluster(prefix string) *endpoint_cluster {
	return &endpoint_cluster{endpoints: map[string]*endpoint{}, prefix: prefix}
}

func (self *endpoint_cluster) delete(key string) {
	delete(self.endpoints, key)
}

func (self *endpoint_cluster) add_proxy(url string) error {
	self.Lock()
	defer self.Unlock()
	e, err := newEndpoint(url)
	if err != nil {
		logger.Println("Failed to add a new endpoint:", err)
		return err
	}
	if self.endpoints == nil {
		self.endpoints = map[string]*endpoint{url: e}
	} else {
		self.endpoints[url] = e
	}
	self.current = e
	logger.Printf("[%s]Backend server address updated:%s\n", self.prefix, url)
	return nil
}

func (self *endpoint_cluster) Ping() error {
	self.Lock()
	defer self.Unlock()
	for k, v := range self.endpoints {
		if err := v.Ping(); err != nil {
			delete(self.endpoints, k)
		}
	}
	if len(self.endpoints) == 0 {
		return errors.New("Empty endpoint_cluster.")
	}
	return nil
}

func (h *endpoint_cluster) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	atomic.AddUint64(&counter, uint64(1))
	h.Lock()
	defer h.Unlock()
	if len(h.endpoints) == 0 {
		http.Error(w, "No server registered.", 500)
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
			http.Error(w, "No server registered.", 500)
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

type Handle struct {
	sync.Mutex
	url_str   string
	url       *url.URL
	endpoints map[string]*endpoint_cluster
}

func (h *Handle) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	atomic.AddUint64(&counter, uint64(1))
	h.Lock()
	defer h.Unlock()
	if len(h.endpoints) == 0 {
		http.Error(w, "No server registered.", 500)
		return
	}
	longest := "/"
	for k, _ := range h.endpoints {
		if strings.HasPrefix(r.URL.Path, k) {
			if len(k) > len(longest) {
				longest = k
			}
		}
	}
	if _, ok := h.endpoints[longest]; !ok {
		http.Error(w, "No handler matched.", 500)
	}
	h.endpoints[longest].ServeHTTP(w, r)
	return
}

func (h *Handle) Update(req *[]string, rep *error) error {
	var err error
	h.Lock()
	request := *req
	defer h.Unlock()
	if h.endpoints == nil {
		h.endpoints = map[string]*endpoint_cluster{}
	}
	if len(request) != 2 {
		err = errors.New("Invalid parameters!")
		*rep = err
		return err
	}
	if _, ok := h.endpoints[(request)[0]]; !ok {
		h.endpoints[request[0]] = newCluster(request[0])
	}
	err = h.endpoints[request[0]].add_proxy(request[1])
	return err
}

func (h *Handle) Delete(req *[]string, rep *error) error {
	h.Lock()
	defer h.Unlock()
	request := *req
	logger.Println("Backend server unsigned:", *req)
	if _, ok := h.endpoints[request[0]]; !ok {
		return nil
	}
	h.endpoints[request[0]].delete(request[1])
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
