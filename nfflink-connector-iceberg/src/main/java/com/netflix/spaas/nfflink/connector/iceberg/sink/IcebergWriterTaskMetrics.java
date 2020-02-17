package com.netflix.spaas.nfflink.connector.iceberg.sink;

import com.netflix.spaas.nfflink.connector.iceberg.IcebergConnectorConstant;
import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Timer;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * This is the taskmanager-level metrics for writer.
 *
 * Since this class uses singleton pattern, be careful when using PolledMeter.
 * https://github.com/Netflix/spectator/blob/master/docs/intro/gauge.md
 *
 * @see IcebergWriterSubtaskMetrics
 */
public class IcebergWriterTaskMetrics {

    private final Registry registry;
    private final String database;
    private final String table;

    private final Timer s3UploadLatency;

    private IcebergWriterTaskMetrics(final Registry registry, final String database, final String table) {
        this.registry = registry;
        this.database = database;
        this.table = table;

        s3UploadLatency = registry.timer(createId("iceberg_sink.s3_upload_latency"));
    }

    private static final Map<String, IcebergWriterTaskMetrics> INSTANCES = new HashMap<>();

    public static IcebergWriterTaskMetrics getInstance(final Registry registry, final String database, final String table) {
        synchronized (INSTANCES) {
            final String key = database + "-" + table;
            if (!INSTANCES.containsKey(key)) {
                INSTANCES.put(key, new IcebergWriterTaskMetrics(registry, database, table));
            }
            return INSTANCES.get(key);
        }
    }

    private Id createId(final String name) {
        return registry.createId(name)
                .withTag(IcebergConnectorConstant.SINK_TAG_KEY, IcebergConnectorConstant.TYPE)
                .withTag(IcebergConnectorConstant.OUTPUT_TAG_KEY, table)
                .withTag(IcebergConnectorConstant.OUTPUT_CLUSTER_TAG_KEY, database);
    }

    public Registry getRegistry() {
        return registry;
    }

    public void recordS3UploadLatency(final long amount, final TimeUnit timeUnit) {
        s3UploadLatency.record(amount, timeUnit);
    }
}
