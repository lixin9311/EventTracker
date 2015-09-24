package main

import (
	"errors"
	"flag"
	et "github.com/lixin9311/EventTracker/eventtracker"
	"github.com/lixin9311/lfshook"
	"github.com/lixin9311/logrus"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/http/httputil"
	"net/rpc"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	handle     = new(Handle)
	log        *logrus.Logger
	conf       *et.Config
	configFile = flag.String("c", "config.json", "Config file in json.")
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
		log.WithFields(logrus.Fields{
			"module": "front",
		}).Errorln("Failed to add a new endpoint:", err)
		return err
	}
	if self.endpoints == nil {
		self.endpoints = map[string]*endpoint{url: e}
	} else {
		self.endpoints[url] = e
	}
	self.current = e
	log.WithFields(logrus.Fields{
		"module": "front",
	}).Infof("[%s]Backend server address updated:%s\n", self.prefix, url)
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
		log.WithFields(logrus.Fields{
			"module": "front",
		}).Errorln("No server registered.")
		http.Error(w, "No server registered.", 500)
		return
	}
	if *balance {
		goto BALANCE
	} else {
		if h.current != nil {
			if err := h.current.Ping(); err != nil {
				log.WithFields(logrus.Fields{
					"module": "front",
				}).Warnln("Current endpoint at: ", h.current.url, " may not functioning, fallback:", err)
				goto FALLBACK
			}
			h.current.ServeHTTP(w, r)
		} else {
			log.WithFields(logrus.Fields{
				"module": "front",
			}).Errorln("No server registered.")
			http.Error(w, "No server registered.", 500)
			return
		}
	}
	return

BALANCE:
	{
		for k, v := range h.endpoints {
			if err := v.Ping(); err != nil {
				log.WithFields(logrus.Fields{
					"module": "front",
				}).Debugln("Server at: ", k, "is not available. delete it.")
				delete(h.endpoints, k)
				continue
			}
			log.WithFields(logrus.Fields{
				"module": "front",
			}).Debugln("Using the server at: ", k)
			v.ServeHTTP(w, r)
			return
		}
		log.WithFields(logrus.Fields{
			"module": "front",
		}).Errorln("All server is unavailable.")
		http.Error(w, "No server available.", 500)
		return
	}
FALLBACK:
	{
		for k, v := range h.endpoints {
			if err := v.Ping(); err != nil {
				log.WithFields(logrus.Fields{
					"module": "front",
				}).Debugln("Server at: ", k, "is not available. delete it.")
				delete(h.endpoints, k)
				continue
			}
			log.WithFields(logrus.Fields{
				"module": "front",
			}).Debugln("Using the server at: ", k)
			h.current = v
			v.ServeHTTP(w, r)
			return
		}
		log.WithFields(logrus.Fields{
			"module": "front",
		}).Errorln("All server is unavailable.")
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
		log.WithFields(logrus.Fields{
			"module": "front",
		}).Errorln("No service has ever registered.")
		http.Error(w, "No service registered.", 500)
		return
	}
	longest := ""
	for k, _ := range h.endpoints {
		if strings.HasPrefix(r.URL.Path, k) {
			if len(k) > len(longest) {
				longest = k
			}
		}
	}
	if _, ok := h.endpoints[longest]; !ok {
		log.WithFields(logrus.Fields{
			"module": "front",
		}).Errorln("No service matches the request.")
		http.Error(w, "No handler matched.", 500)
		return
	}
	log.WithFields(logrus.Fields{
		"module": "front",
	}).Debugln("Found service:", longest)
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
		log.WithFields(logrus.Fields{
			"module": "front",
		}).Errorln("Remote Service Register recieved invalid parameters:", request)
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
	log.WithFields(logrus.Fields{
		"module": "front",
	}).Infoln("Backend server unsigned:", *req)
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
	log.WithFields(logrus.Fields{
		"module": "front",
	}).Infoln("Front server start, http service listening:", conf.Main.Http_listen_addr)
	err := http.ListenAndServe(conf.Main.Http_listen_addr, handle)
	if err != nil {
		log.WithFields(logrus.Fields{
			"module": "front",
		}).Fatalln("Failed to bring up main service:", err)
	}
}

func startRPC() {
	rpcServer := rpc.NewServer()
	rpcServer.Register(handle)
	l, err := net.Listen("tcp", conf.Front.Service_reg_addr)
	if err != nil {
		log.WithFields(logrus.Fields{
			"module": "front",
		}).Fatalln("Failed to start Remote Service Register service:", err)
	}
	log.WithFields(logrus.Fields{
		"module": "front",
	}).Infoln("Front server started, reg service listening:", conf.Front.Service_reg_addr)
	rpcServer.Accept(l)
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
		log.WithFields(logrus.Fields{
			"module": "front",
		}).Fatalln("Unrecognized log file formatter:", conf.Main.Log_file_formatter)
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
		log.WithFields(logrus.Fields{
			"module": "front",
		}).Fatalln("Unrecognized log level:", conf.Main.Log_level)
	}
	if !conf.Front.Enabled {
		log.WithFields(logrus.Fields{
			"module": "front",
		}).Fatalln("Front service is not enabled.")
	}
	log.WithFields(logrus.Fields{
		"module": "front",
	}).Infoln("Front service initialization complete.")
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
