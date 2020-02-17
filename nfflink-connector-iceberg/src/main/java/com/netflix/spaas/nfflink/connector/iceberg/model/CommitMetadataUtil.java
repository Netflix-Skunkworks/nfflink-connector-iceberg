package com.netflix.spaas.nfflink.connector.iceberg.model;

import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;

/**
 * Companion class with CommitMetadata (Avro generated SpecificRecord)
 */
public class CommitMetadataUtil {
    private static final Logger LOG = LoggerFactory.getLogger(CommitMetadataUtil.class);
    private static final CommitMetadataUtil INSTANCE = new CommitMetadataUtil();

    public static CommitMetadataUtil getInstance() {
        return INSTANCE;
    }

    private final DatumWriter<CommitMetadata> datumWriter;

    private CommitMetadataUtil() {
        datumWriter = new SpecificDatumWriter<>(CommitMetadata.class);
    }

    public String encodeAsJson(CommitMetadata metadata) {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try {
            Encoder jsonEncoder = EncoderFactory.get().jsonEncoder(CommitMetadata.getClassSchema(), outputStream);
            datumWriter.write(metadata, jsonEncoder);
            jsonEncoder.flush();
            return new String(outputStream.toByteArray());
        } catch (Exception e) {
            LOG.error("failed to encode metadata to JSON", e);
            return "";
        }
    }

}