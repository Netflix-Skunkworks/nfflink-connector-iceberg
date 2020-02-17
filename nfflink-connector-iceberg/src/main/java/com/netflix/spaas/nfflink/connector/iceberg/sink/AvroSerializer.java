package com.netflix.spaas.nfflink.connector.iceberg.sink;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;

import java.io.Serializable;

/**
 * Serialize input data type to Avro IndexedRecord
 */
@FunctionalInterface
public interface AvroSerializer<IN> extends Serializable {
    IndexedRecord serialize(IN record, Schema avroSchema) throws Exception;
}
