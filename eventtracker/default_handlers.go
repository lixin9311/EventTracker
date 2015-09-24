package eventtracker

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"github.com/lixin9311/logrus"
	"html/template"
	"io"
	"log"
	"net/http"
	"strconv"
)

type DefaultHandler struct {
	logger *logrus.Logger
	// MaxFileSize is the maximum size of upload file
	MaxFileSize int64
	// MaxMemorySize is the maximum memory size to handle the upload file
	MaxMemorySize int64
	fail_safe     *log.Logger
	kafka         *Kafka
	avro          *Avro
}

func NewDefaultHandler(w *logrus.Logger, fail_safe *log.Logger, kafka *Kafka, avro *Avro) *DefaultHandler {
	return &DefaultHandler{logger: w, MaxFileSize: int64(10 * 1024 * 1024), MaxMemorySize: int64(10 * 1024 * 1024), fail_safe: fail_safe, kafka: kafka, avro: avro}
}

func (self *DefaultHandler) PingHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Pong")
}

func PingHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Pong")
}

// HomeHandler is the index page for upload the csv file
func (self *DefaultHandler) HomeHandler(w http.ResponseWriter, r *http.Request) {
	t, err := template.ParseFiles("index.html")
	if err != nil {
		self.logger.WithFields(logrus.Fields{
			"module": "Handler",
		}).Fatalln("Parse index template failed:", err)
	}
	t.Execute(w, nil)
}

// ErrorAndReturnCode prints an error and reponse to http client
func (self *DefaultHandler) ErrorAndReturnCode(w http.ResponseWriter, errstr string, code int) {
	self.logger.WithFields(logrus.Fields{
		"module": "Handler",
	}).Errorln(errstr)
	http.Error(w, errstr, code)
}

// UploadHandler handles the upload file
func (self *DefaultHandler) UploadHandler(w http.ResponseWriter, r *http.Request) {
	var remote string
	if tmp := r.Header.Get("X-Forwarded-For"); tmp != "" {
		remote = tmp
	} else {
		remote = r.RemoteAddr
	}
	self.logger.WithFields(logrus.Fields{
		"module": "Handler",
	}).Debugln("Incomming upload file from:", remote, "With Header:", r.Header)
	// limit the file size
	if r.ContentLength > self.MaxFileSize {
		self.ErrorAndReturnCode(w, "The file is too large:"+strconv.FormatInt(r.ContentLength, 10)+"bytes", 400)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, self.MaxFileSize)
	err := r.ParseMultipartForm(self.MaxMemorySize)
	if err != nil {
		self.ErrorAndReturnCode(w, "Failed to parse form:"+err.Error(), 500)
		return
	}
	// get the file
	file, _, err := r.FormFile("uploadfile")
	if err != nil {
		self.ErrorAndReturnCode(w, "Failed to read upload file:"+err.Error(), 500)
		return
	}
	defer file.Close()
	// read the file
	csvreader := csv.NewReader(file)
	record, err := csvreader.Read()
	if err != nil {
		self.ErrorAndReturnCode(w, "Failed to read the first line of file:"+err.Error(), 500)
		return
	}
	title := map[string]int{}
	ext := map[string]int{}
	extension := map[string](interface{}){}
	// read title and extensions
	for k, v := range record {
		if v == "did" || v == "aid" || v == "ip" || v == "timestamp" {
			title[v] = k
		} else {
			ext[v] = k
		}
	}
	if _, ok := title["did"]; !ok {
		self.ErrorAndReturnCode(w, "Missing Required field: No did", 400)
		return
	}
	if _, ok := title["timestamp"]; !ok {
		self.ErrorAndReturnCode(w, "Missing Required field: No timestamp", 400)
		return
	}
	if _, ok := ext["event_type"]; !ok {
		self.ErrorAndReturnCode(w, "Missing Required field: No event_type", 400)
		return
	}
	// read the record one by one and send it to kafka
	buf := new(bytes.Buffer)
	counter := 0
	for {
		// one more line
		record, err := csvreader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			self.ErrorAndReturnCode(w, "Err read file:"+err.Error(), 500)
			return
		}
		arecord, err := self.avro.NewRecord()
		if err != nil {
			self.ErrorAndReturnCode(w, "Failed to set new avro record:"+err.Error(), 500)
			return
		}
		// set main title
		for k, v := range title {
			arecord.Set(k, record[v])
		}
		// set ext map
		for k, v := range ext {
			extension[k] = record[v]
		}
		if len(extension) != 0 {
			arecord.Set("extension", extension)
		}
		// fullfill the event.avsc required fields
		arecord.Set("event", "TrackerEvent")
		arecord.Set("id", "")
		// encode avro
		if err = self.avro.Encode(buf, arecord); err != nil {
			self.ErrorAndReturnCode(w, "Failed to encode avro record:"+err.Error(), 500)
			return
		}
		// send to kafka
		_, _, err = self.kafka.SendByteMessage(buf.Bytes(), record[ext["event_type"]])
		if err != nil {
			self.fail_safe.Println("error:", err)
			self.fail_safe.Println("record:", arecord)
			self.fail_safe.Println("data:", buf.Bytes())
			self.ErrorAndReturnCode(w, "Failed to send to kafka:"+err.Error(), 500)
			return
		}
		buf.Reset()
		counter++
	}
	// done
	self.logger.WithFields(logrus.Fields{
		"module": "Handler",
	}).Debugln("%d messages have been writen.", counter)
	w.WriteHeader(200)
	fmt.Fprintf(w, "%d messages have been writen.", counter)
}

// EventHandler is the REST api handler
func (self *DefaultHandler) EventHandler(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	var remote string
	if tmp := r.Header.Get("X-Forwarded-For"); tmp != "" {
		remote = tmp
	} else {
		remote = r.RemoteAddr
	}
	self.logger.WithFields(logrus.Fields{
		"module": "Handler",
	}).Debugln("Incomming event from:", remote, "With Header:", r.Header)
	// required fields
	if len(r.Form["did"]) < 1 {
		self.ErrorAndReturnCode(w, "Missing Required field: No did", 400)
		return
	}
	if len(r.Form["timestamp"]) < 1 {
		self.ErrorAndReturnCode(w, "Missing Required field: No timestamp", 400)
		return
	}
	if len(r.Form["event_type"]) < 1 {
		self.ErrorAndReturnCode(w, "Missing Required field: No event_type", 400)
		return
	}
	// set a new avro record
	record, err := self.avro.NewRecord()
	if err != nil {
		self.ErrorAndReturnCode(w, "Failed to set a new avro record:"+err.Error(), 500)
		return
	}
	// optional fields
	if len(r.Form["ip"]) > 0 {
		record.Set("ip", r.Form["ip"][0])
	}
	if len(r.Form["aid"]) > 0 {
		record.Set("aid", r.Form["aid"][0])
	}
	// set required fields
	record.Set("did", r.Form["did"][0])
	record.Set("timestamp", r.Form["timestamp"][0])
	record.Set("event", "TrackerEvent")
	record.Set("id", "")
	// extensions fields
	extension := map[string](interface{}){}
	for k, v := range r.Form {
		if k != "ip" && k != "aid" && k != "did" && k != "timestamp" {
			extension[k] = v[0]
		}
	}
	if len(extension) != 0 {
		record.Set("extension", extension)
	}
	self.logger.WithFields(logrus.Fields{
		"module": "Handler",
	}).Debugln("Generated AVRO record:", record)
	// encode avro
	buf := new(bytes.Buffer)
	if err = self.avro.Encode(buf, record); err != nil {
		self.ErrorAndReturnCode(w, "Failed to encode avro record:"+err.Error(), 500)
		return
	}
	// send to kafka
	part, offset, err := self.kafka.SendByteMessage(buf.Bytes(), r.Form["event_type"][0])
	if err != nil {
		self.fail_safe.Println("error:", err)
		self.fail_safe.Println("record:", record)
		self.fail_safe.Println("data:", buf.Bytes())
		self.ErrorAndReturnCode(w, "Failed to send message to kafka:"+err.Error()+"Data has been writen to a backup file. Please contact us.", 500)
		return
	}
	// done
	self.logger.WithFields(logrus.Fields{
		"module": "Handler",
	}).Debugf("New record partition=%d\toffset=%d\n", part, offset)
	w.WriteHeader(200)
	fmt.Fprintf(w, "1 messages have been writen.")
}
