package com.netflix.spaas.nfflink.connector.iceberg.sink;

import com.netflix.spaas.nfflink.connector.iceberg.IcebergConnectorConstant;
import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Gauge;
import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;

/**
 * This is the subtask/slot-level metrics for writer.
 *
 * @see IcebergWriterTaskMetrics
 */
public class IcebergWriterSubtaskMetrics {
    private final Registry registry;
    private final String database;
    private final String table;
    private final int subtaskId;

    // writer metrics
    private final Counter receivedRecords;
    private final Counter writtenRecords;
    private final Id failedRecordsId;
    private final Counter uploadedFiles;
    private final Counter uploadedRecords;
    private final Counter uploadedBytes;
    private final Counter emittedFiles;
    private final Counter emittedRecords;
    private final Counter emittedBytes;
    private final Counter avroSerializerFailures;
    private final Counter icebergAppendRecordTypeFailures;
    private final Counter icebergAppendRecordIOFailures;
    private final Gauge openFileCountGauge;

    public IcebergWriterSubtaskMetrics(final Registry registry, final String database,
                                        final String table, final int subtaskId) {
        this.registry = registry;
        this.database = database;
        this.table = table;
        this.subtaskId = subtaskId;

        receivedRecords = registry.counter(createId("iceberg_sink.writer_received_records"));
        writtenRecords = registry.counter(createId("iceberg_sink.written_records"));
        failedRecordsId = createId("iceberg_sink.failed_records");
        uploadedFiles = registry.counter(createId("iceberg_sink.uploaded_files"));
        uploadedRecords = registry.counter(createId("iceberg_sink.uploaded_records"));
        uploadedBytes = registry.counter(createId("iceberg_sink.uploaded_bytes"));
        emittedFiles = registry.counter(createId("iceberg_sink.writer_emitted_files"));
        emittedRecords = registry.counter(createId("iceberg_sink.writer_emitted_records"));
        emittedBytes = registry.counter(createId("iceberg_sink.writer_emitted_bytes"));
        avroSerializerFailures = registry.counter(createId("iceberg_sink.avro_serializer_failures"));
        icebergAppendRecordTypeFailures = registry.counter(createId("iceberg_sink.iceberg_append_record_type_failures"));
        icebergAppendRecordIOFailures = registry.counter(createId("iceberg_sink.iceberg_append_record_io_failures"));
        openFileCountGauge = registry.gauge(createId("iceberg_sink.open_file_count"));
    }

    private Id createId(final String name) {
        return registry.createId(name)
                .withTag(IcebergConnectorConstant.SINK_TAG_KEY, IcebergConnectorConstant.TYPE)
                .withTag(IcebergConnectorConstant.OUTPUT_TAG_KEY, table)
                .withTag(IcebergConnectorConstant.OUTPUT_CLUSTER_TAG_KEY, database)
                .withTag(IcebergConnectorConstant.SUBTASK_ID, Integer.toString(subtaskId));
    }

    public void incrementReceivedRecords() {
        receivedRecords.increment();
    }

    public void incrementWrittenRecords() {
        writtenRecords.increment();
    }

    public void incrementFailedRecords(final Throwable t) {
        Id id = failedRecordsId.withTag(IcebergConnectorConstant.EXCEPTION_CLASS, t.getClass().getSimpleName());
        registry.counter(id).increment();
    }

    public void incrementUploadedFiles() {
        uploadedFiles.increment();
    }

    public void incrementUploadedRecords(final long delta) {
        uploadedRecords.increment(delta);
    }

    public void incrementUploadedBytes(final long delta) {
        uploadedBytes.increment(delta);
    }

    public void incrementWriterEmittedFiles() {
        emittedFiles.increment();
    }

    public void incrementWriterEmittedRecords(final long delta) {
        emittedRecords.increment(delta);
    }

    public void incrementWriterEmittedBytes(final long delta) {
        emittedBytes.increment(delta);
    }

    public void incrementAvroSerializerFailures() {
        avroSerializerFailures.increment();
    }

    public void incrementIcebergAppendRecordTypeFailures() {
        icebergAppendRecordTypeFailures.increment();
    }

    public void incrementIcebergAppendRecordIOFailures() {
        icebergAppendRecordIOFailures.increment();
    }

    public void incrementOpenFileCount() {
        openFileCountGauge.set(openFileCountGauge.value() + 1);
    }

    public void resetOpenFileCount() {
        openFileCountGauge.set(0);
    }

}
