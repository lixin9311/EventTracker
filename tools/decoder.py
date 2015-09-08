#coding: utf-8
import avro.schema
from avro.io import DatumReader, DatumWriter, BinaryEncoder
from avro.datafile import DataFileWriter 

from kafka.client import KafkaClient
from kafka.consumer import SimpleConsumer
from kafka.producer import SimpleProducer

import json
import io

schema = avro.schema.parse(open("../event.avsc").read())

reader = avro.io.DatumReader(schema)

print "connect"

client = KafkaClient("localhost:9092")
consumer = SimpleConsumer(client, "default", "default")

print "start"

from  direct_data_file_writer import DirectDataFileWriter

a = DirectDataFileWriter(open("a.avro", "w"), DatumWriter(), schema)
b = DataFileWriter(open("b.avro", "w"), DatumWriter(), schema)
c = open("c.avro", "w")

index = 0
for message in consumer:
    a.append_encoded_datum(message.message.value, 2)
    c.write(message.message.value)
    bytes_reader = io.BytesIO(message.message.value)
    decoder = avro.io.BinaryDecoder(bytes_reader)
    obj = reader.read(decoder)
    b.append(obj)
    bytes_reader.close()
    print message.message.key, obj
    index += 1
    print index

a.close()
b.close()
c.close()
