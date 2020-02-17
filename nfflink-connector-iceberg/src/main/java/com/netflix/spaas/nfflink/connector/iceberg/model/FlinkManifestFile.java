package com.netflix.spaas.nfflink.connector.iceberg.model;

import org.apache.iceberg.ManifestFile;

public interface FlinkManifestFile extends ManifestFile {

    long checkpointId();

    long checkpointTimestamp();

    long dataFileCount();

    long recordCount();

    long byteCount();

    Long lowWatermark();

    Long highWatermark();

    /**
     * if implementation of this method changed,
     * it may affect de-dup check and cause the same manifest file be committed twice.
     */
    String hash();

    ManifestFileState toState();
}
