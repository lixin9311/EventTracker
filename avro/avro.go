package avro

import (
	"github.com/linkedin/goavro"
	"io"
	"io/ioutil"
	"log"
)

var (
	codec            goavro.Codec
	recordSchemaJSON string
	logger           *log.Logger
)

// Init initializes a avro package
func Init(w io.Writer, setting map[string]string) {
	logger = log.New(w, "[avro]:", log.LstdFlags|log.Lshortfile)
	loadSchema(setting["schema"])
}

// init avro with a schema
func loadSchema(path string) {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		logger.Fatalln("Failed to open schema file:", err)
	}
	recordSchemaJSON = string(data)
	codec, err = goavro.NewCodec(recordSchemaJSON)
	if err != nil {
		logger.Fatalln("Failed to init codec from schema:", err)
	}
}

// NewRecord inits a new record
func NewRecord() (*goavro.Record, error) {
	record, err := goavro.NewRecord(goavro.RecordSchema(recordSchemaJSON))
	return record, err
}

// Encode encodes a record
func Encode(w io.Writer, data *goavro.Record) error {
	return codec.Encode(w, data)
}

// Decode decodes a record
func Decode(r io.Reader) (*goavro.Record, error) {
	record, err := codec.Decode(r)
	return record.(*goavro.Record), err
}
