package com.netflix.spaas.nfflink.connector.iceberg.sink;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;

public class PassThroughAvroSerializer implements AvroSerializer<IndexedRecord> {

    private static final PassThroughAvroSerializer INSTANCE = new PassThroughAvroSerializer();

    public static PassThroughAvroSerializer getInstance() {
        return INSTANCE;
    }

    public IndexedRecord serialize(IndexedRecord record, Schema avroSchema) {
        return record;
    }
}
