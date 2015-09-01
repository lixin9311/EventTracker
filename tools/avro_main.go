package main

import (
	"./avro"
	"bytes"
	"fmt"
	"log"
	//	"strconv"
)

func main() {
	err := avro.LoadSchema("./event.avsc")
	if err != nil {
		log.Fatalln("err load schema:", err)
	}
	record, err := avro.NewRecord()
	if err != nil {
		log.Fatalln(err)
	}
	record.Set("id", "www")

	record.Set("event", "")
	record.Set("timestamp", "www")
	record.Set("exchange", nil)
	fmt.Println(record)
	buf := new(bytes.Buffer)
	buf2 := new(bytes.Buffer)
	if err = avro.Encode(buf, record); err != nil {
		log.Fatalln(err)
	}
	if err = avro.Encode(buf2, record); err != nil {
		log.Fatalln(err)
	}
	fmt.Printf("%2x\n", buf2.Bytes())
	decode, err := avro.Decode(buf)
	fmt.Println("Record Name:", decode.Name)
	fmt.Println("Record Fields:")
	for i, field := range decode.Fields {
		fmt.Println(" field", i, field.Name, ":", field.Datum)
	}
}
