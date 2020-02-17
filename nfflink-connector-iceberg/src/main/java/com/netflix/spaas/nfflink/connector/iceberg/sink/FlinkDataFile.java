package com.netflix.spaas.nfflink.connector.iceberg.sink;

import com.google.common.base.MoreObjects;
import org.apache.iceberg.DataFile;

import java.io.Serializable;

public class FlinkDataFile implements Serializable {
    private final long lowWatermark;
    private final long highWatermark;
    private final DataFile dataFile;

    public FlinkDataFile(long lowWatermark, long highWatermark, DataFile dataFile) {
        this.lowWatermark = lowWatermark;
        this.highWatermark = highWatermark;
        this.dataFile = dataFile;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("low_watermark", lowWatermark)
                .add("high_watermark", highWatermark)
                .add("data_file", dataFile)
                .toString();
    }

    /**
     * only dump essential fiels like lowTimestamp, highTimestamp,and path
     */
    public String toCompactDump() {
        return MoreObjects.toStringHelper(this)
                .add("low_watermark", lowWatermark)
                .add("high_watermark", highWatermark)
                .add("path", dataFile.path())
                .toString();
    }

    public DataFile getIcebergDataFile() {
        return dataFile;
    }

    public long getLowWatermark() {
        return lowWatermark;
    }

    public long getHighWatermark() {
        return highWatermark;
    }
}
