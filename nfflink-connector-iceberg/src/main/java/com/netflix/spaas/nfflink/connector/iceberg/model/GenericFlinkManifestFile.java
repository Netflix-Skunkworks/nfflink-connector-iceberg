package com.netflix.spaas.nfflink.connector.iceberg.model;

import com.google.common.base.MoreObjects;
import org.apache.commons.codec.binary.Hex;
import org.apache.iceberg.ManifestFile;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Objects;

public class GenericFlinkManifestFile implements FlinkManifestFile {

  private final String path;
  private final long length;
  private final int specId;
  private final long checkpointId;
  private final long checkpointTimestamp;
  private final long dataFileCount;
  private final long recordCount;
  private final long byteCount;
  private final Long lowWatermark;
  private final Long highWatermark;

  public static class Builder {

    private String path;
    private long length;
    private int specId;
    private long checkpointId;
    private long checkpointTimestamp;
    private long dataFileCount;
    private long recordCount;
    private long byteCount;
    private Long lowWatermark;
    private Long highWatermark;

    private Builder() {

    }

    public Builder setPath(String path) {
      this.path = path;
      return this;
    }

    public Builder setLength(long length) {
      this.length = length;
      return this;
    }

    public Builder setSpecId(int specId) {
      this.specId = specId;
      return this;
    }

    public Builder setCheckpointId(long checkpointId) {
      this.checkpointId = checkpointId;
      return this;
    }

    public Builder setCheckpointTimestamp(long checkpointTimestamp) {
      this.checkpointTimestamp = checkpointTimestamp;
      return this;
    }

    public Builder setDataFileCount(long dataFileCount) {
      this.dataFileCount = dataFileCount;
      return this;
    }

    public Builder setRecordCount(long recordCount) {
      this.recordCount = recordCount;
      return this;
    }

    public Builder setByteCount(long byteCount) {
      this.byteCount = byteCount;
      return this;
    }

    public Builder setLowWatermark(Long lowWatermark) {
      this.lowWatermark = lowWatermark;
      return this;
    }

    public Builder setHighWatermark(Long highWatermark) {
      this.highWatermark = highWatermark;
      return this;
    }

    public GenericFlinkManifestFile build() {
      return new GenericFlinkManifestFile(this);
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  private GenericFlinkManifestFile(Builder builder) {
    path = builder.path;
    length = builder.length;
    specId = builder.specId;
    checkpointId = builder.checkpointId;
    checkpointTimestamp = builder.checkpointTimestamp;
    dataFileCount = builder.dataFileCount;
    recordCount = builder.recordCount;
    byteCount = builder.byteCount;
    lowWatermark = builder.lowWatermark;
    highWatermark = builder.highWatermark;
  }


  public static GenericFlinkManifestFile fromState(ManifestFileState state) {
    return GenericFlinkManifestFile.builder()
            .setPath(state.getPath().toString())
            .setLength(state.getLength())
            .setSpecId(state.getSpecId())
            .setCheckpointId(state.getCheckpointId())
            .setCheckpointTimestamp(state.getCheckpointTimestamp())
            .setDataFileCount(state.getDataFileCount())
            .setRecordCount(state.getRecordCount())
            .setByteCount(state.getByteCount())
            .setLowWatermark(state.getLowWatermark())
            .setHighWatermark(state.getHighWatermark())
            .build();
  }

  public ManifestFileState toState() {
    return ManifestFileState.newBuilder()
            .setPath(path)
            .setLength(length)
            .setSpecId(specId)
            .setCheckpointId(checkpointId)
            .setCheckpointTimestamp(checkpointTimestamp)
            .setDataFileCount(dataFileCount)
            .setRecordCount(recordCount)
            .setByteCount(byteCount)
            .setLowWatermark(lowWatermark)
            .setHighWatermark(highWatermark)
            .build();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(getClass())
            .add("path", path)
            .add("length", length)
            .add("specId", specId)
            .add("checkpointId", checkpointId)
            .add("checkpointTimestamp", checkpointTimestamp)
            .add("dataFileCount", dataFileCount)
            .add("recordCount", recordCount)
            .add("byteCount", byteCount)
            .add("lowWatermark", lowWatermark)
            .add("highWatermark", highWatermark)
            .add("hash", hash())
            .toString();
  }

  @Override
  public boolean equals(Object obj) {
      if (obj == null) return false;
      if (getClass() != obj.getClass()) return false;
      final GenericFlinkManifestFile other = (GenericFlinkManifestFile) obj;
      return Objects.equals(this.path, other.path)
              && Objects.equals(this.length, other.length)
              && Objects.equals(this.specId, other.specId)
              && Objects.equals(this.checkpointId, other.checkpointId)
              && Objects.equals(this.checkpointTimestamp, other.checkpointTimestamp)
              && Objects.equals(this.dataFileCount, other.dataFileCount)
              && Objects.equals(this.recordCount, other.recordCount)
              && Objects.equals(this.lowWatermark, other.lowWatermark)
              && Objects.equals(this.highWatermark, other.highWatermark)
              ;
  }

  @Override
  public String path() {
    return path;
  }

  @Override
  public long length() {
    return length;
  }

  @Override
  public int partitionSpecId() {
    return specId;
  }

  @Override
  public Long snapshotId() {
    return null;
  }

  @Override
  public Integer addedFilesCount() {
    return null;
  }

  @Override
  public Integer existingFilesCount() {
    return null;
  }

  @Override
  public Integer deletedFilesCount() {
    return null;
  }

  @Override
  public List < PartitionFieldSummary > partitions() {
    return null;
  }

  @Override
  public ManifestFile copy() {
    return null;
  }

  @Override
  public long checkpointId() {
    return checkpointId;
  }

  @Override
  public long checkpointTimestamp() {
    return checkpointTimestamp;
  }

  @Override
  public long dataFileCount() {
    return dataFileCount;
  }

  @Override
  public long recordCount() {
    return recordCount;
  }

  @Override
  public long byteCount() {
    return byteCount;
  }

  @Override
  public Long lowWatermark() {
    return lowWatermark;
  }

  @Override
  public Long highWatermark() {
    return highWatermark;
  }

  @Override
  public String hash() {
    try {
      MessageDigest messageDigest = MessageDigest.getInstance("SHA-1");
      messageDigest.update(path().getBytes());
      byte[] md = messageDigest.digest();
      return Hex.encodeHexString(md);
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException("failed to create digest", e);
    }
  }
}
