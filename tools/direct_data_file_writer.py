#coding: utf-8
import zlib
from avro.datafile import DataFileWriter, CODEC_KEY, DataFileException

class DirectDataFileWriter(DataFileWriter):
    def __init__(self, writer, datum_writer, writers_schema=None, codec='null'):
        super(DirectDataFileWriter, self).__init__(writer, datum_writer, writers_schema, codec)
    
        self.encoded_block_count = 0
        self.encoded_block_buffer = ""

    def _write_encoded_block(self):
        if not self._header_written:
            self._write_header()

        if self.encoded_block_count > 0:
            #write compressed block count
            self.encoder.write_long(self.encoded_block_count)
            uncompressed_data = self.encoded_block_buffer
            if self.get_meta(CODEC_KEY) == 'null':
                compressed_data = uncompressed_data 
            elif self.get_meta(CODEC_KEY) == 'deflate':
                compressed_data = zlib.compress(uncompressed_data)[2:-1]
            else:
                fail_msg = '"%s" codec is not supported.' % self.get_meta(CODEC_KEY)
                raise DataFileException(fail_msg)
            # write length of block 
            self.encoder.write_long(len(compressed_data))
            # Write block
            self.writer.write(compressed_data)
            # write sync marker 
            self.writer.write(self.sync_marker)
            # reset buffer
            self.encoded_block_buffer = ""
            self.encoded_block_count = 0 

    def append_encoded_datum(self, encoded_datum, sync_interval):
        self.encoded_block_buffer += encoded_datum
        self.encoded_block_count += 1

        if self.encoded_block_count >= sync_interval:
            self._write_encoded_block()

    def flush(self):
        self._write_encoded_block()
        super(DirectDataFileWriter, self).flush()


