package com.netflix.spaas.nfflink.connector.iceberg.model;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class FlinkManifestFileUtil {

    public static long getDataFileCount(List<FlinkManifestFile> flinkManifestFiles) {
        return flinkManifestFiles.stream().map(FlinkManifestFile::dataFileCount)
                .collect(Collectors.summingLong(l -> l));
    }

    public static long getRecordCount(List<FlinkManifestFile> flinkManifestFiles) {
        return flinkManifestFiles.stream().map(FlinkManifestFile::recordCount)
                .collect(Collectors.summingLong(l -> l));
    }

    public static long getByteCount(List<FlinkManifestFile> flinkManifestFiles) {
        return flinkManifestFiles.stream().map(FlinkManifestFile::byteCount)
                .collect(Collectors.summingLong(l -> l));
    }

    public static Long getLowWatermark(List<FlinkManifestFile> flinkManifestFiles) {
        Long min = null;
        for (FlinkManifestFile flinkManifestFile : flinkManifestFiles) {
            Long curr = flinkManifestFile.lowWatermark();
            if ((null == min) || (null != curr && min > curr)) {
                min = curr;
            }
        }
        return min;
    }

    public static Long getHighWatermark(List<FlinkManifestFile> flinkManifestFiles) {
        Long max = null;
        for (FlinkManifestFile flinkManifestFile : flinkManifestFiles) {
            Long curr = flinkManifestFile.highWatermark();
            if ((null == max) || (null != curr && max < curr)) {
                max = curr;
            }
        }
        return max;
    }

    public static String hashesListToString(List<String> hashes) {
        return Joiner.on(",").join(hashes);
    }

    public static List<String> hashesStringToList(String hashesStr) {
        if (Strings.isNullOrEmpty(hashesStr)) {
            return Collections.emptyList();
        } else {
            return Arrays.asList(hashesStr.split(","));
        }
    }
}
