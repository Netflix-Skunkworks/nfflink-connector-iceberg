package com.netflix.spaas.nfflink.connector.iceberg.sink;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.*;
import org.apache.avro.reflect.ReflectDatumWriter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class PojoAvroSerializer<IN> implements AvroSerializer<IN> {

    private static final PojoAvroSerializer INSTANCE = new PojoAvroSerializer();

    public static PojoAvroSerializer getInstance() {
        return INSTANCE;
    }

    private transient BinaryEncoder binaryEncoder;
    private transient BinaryDecoder binaryDecoder;

    @Override
    public GenericRecord serialize(IN value, Schema avroSchema) throws Exception {
        final DatumWriter<IN> datumWriter = new ReflectDatumWriter<>(avroSchema);
        final ByteArrayOutputStream bout = new ByteArrayOutputStream();
        binaryEncoder = EncoderFactory.get().binaryEncoder(bout, binaryEncoder);
        datumWriter.write(value, binaryEncoder);
        binaryEncoder.flush();

        final DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(avroSchema);
        binaryDecoder = DecoderFactory.get().binaryDecoder(bout.toByteArray(), binaryDecoder);
        final GenericRecord avroRecord = new GenericData.Record(avroSchema);
        datumReader.read(avroRecord, binaryDecoder);
        return avroRecord;
    }

}
