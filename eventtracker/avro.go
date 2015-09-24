package eventtracker

import (
	"github.com/linkedin/goavro"
	"github.com/lixin9311/logrus"
	"io"
	"io/ioutil"
)

type Avro struct {
	codec            goavro.Codec
	recordSchemaJSON string
	logger           *logrus.Logger
}

// Init initializes a avro package
func NewAvroInst(w *logrus.Logger, conf avro_config) *Avro {
	data, err := ioutil.ReadFile(conf.Schema)
	if err != nil {
		w.WithFields(logrus.Fields{
			"module": "avro",
		}).Fatalln("Failed to open schema file:", err)
	}
	recordSchemaJSON := string(data)
	codec, err := goavro.NewCodec(recordSchemaJSON)
	if err != nil {
		w.WithFields(logrus.Fields{
			"module": "avro",
		}).Fatalln("Failed to init codec from schema:", err)
	}
	w.WithFields(logrus.Fields{
		"module": "avro",
	}).Println("Init completed.")
	return &Avro{codec: codec, recordSchemaJSON: recordSchemaJSON, logger: w}
}

// NewRecord inits a new record
func (self *Avro) NewRecord() (*goavro.Record, error) {
	record, err := goavro.NewRecord(goavro.RecordSchema(self.recordSchemaJSON))
	return record, err
}

// Encode encodes a record
func (self *Avro) Encode(w io.Writer, data *goavro.Record) error {
	return self.codec.Encode(w, data)
}

// Decode decodes a record
func (self *Avro) Decode(r io.Reader) (*goavro.Record, error) {
	record, err := self.codec.Decode(r)
	return record.(*goavro.Record), err
}
