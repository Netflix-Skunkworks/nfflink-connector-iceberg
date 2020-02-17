package com.netflix.spaas.nfflink.connector.iceberg.sink;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.types.Types;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

public class WatermarkTimeExtractorTest {

    private static final String TABLE_NAME = "my_table";
    private static final String DATEINT = "dateint";
    private static final String HOUR = "hour";
    private static final String EVENT_UTC_MS = "event_utc_ms";
    private static final String METADATA = "__metadata__";
    private static final String INGESTION_TIMESTAMP_MS = "ingestion_timestamp_ms";
    private static final long TIMESTAMP_MS = System.currentTimeMillis();
    private static final String V1_TS_FIELD_NAME = EVENT_UTC_MS;
    private static final String V3_TS_FIELD_NAME = METADATA + "." + INGESTION_TIMESTAMP_MS;

    private final Schema v1Schema = createV1Scehma();
    private final Schema v3Schema = createV3Scehma();

    private Schema createV1Scehma() {
        final Types.StructType struct = Types.StructType.of(
                Types.NestedField.optional(1, DATEINT, Types.IntegerType.get()),
                Types.NestedField.optional(2, HOUR, Types.IntegerType.get()),
                Types.NestedField.optional(3, EVENT_UTC_MS, Types.LongType.get())
        );
        org.apache.iceberg.Schema icebergSchema = new org.apache.iceberg.Schema(struct.fields());
        return AvroSchemaUtil.convert(icebergSchema, TABLE_NAME);
    }

    private GenericRecord createV1Record(Schema schema) {
        GenericRecord avroRecord = new GenericData.Record(schema);
        avroRecord.put(DATEINT, 2020);
        avroRecord.put(HOUR, 0);
        avroRecord.put(EVENT_UTC_MS, TIMESTAMP_MS);
        return avroRecord;
    }

    private Schema createV3Scehma() {
        final Types.StructType struct = Types.StructType.of(
                Types.NestedField.optional(1, DATEINT, Types.IntegerType.get()),
                Types.NestedField.optional(2, HOUR, Types.IntegerType.get()),
                Types.NestedField.optional(3, METADATA,
                        Types.StructType.of(
                                Types.NestedField.optional(4, INGESTION_TIMESTAMP_MS, Types.LongType.get())
                        ))
        );
        org.apache.iceberg.Schema icebergSchema = new org.apache.iceberg.Schema(struct.fields());
        return AvroSchemaUtil.convert(icebergSchema, TABLE_NAME);
    }

    private GenericRecord createV3Record(Schema schema) {
        Schema metadataSchema = AvroUtiils.getActualSchema(schema.getField(METADATA).schema());
        GenericRecord metadataRecord = new GenericData.Record(metadataSchema);
        metadataRecord.put(INGESTION_TIMESTAMP_MS, TIMESTAMP_MS);

        GenericRecord avroRecord = new GenericData.Record(schema);
        avroRecord.put(DATEINT, 2020);
        avroRecord.put(HOUR, 0);
        avroRecord.put(METADATA, metadataRecord);
        return avroRecord;
    }

    @Test
    public void testNullConfig() {
        WatermarkTimeExtractor extractor = new WatermarkTimeExtractor(v1Schema, null, TimeUnit.MILLISECONDS);
        GenericRecord avroRecord = createV1Record(v1Schema);
        Long ts = extractor.getWatermarkTimeMs(avroRecord);
        Assert.assertNull(ts);
    }

    @Test
    public void testV1Schema() {
        WatermarkTimeExtractor extractor = new WatermarkTimeExtractor(v1Schema, V1_TS_FIELD_NAME, TimeUnit.MILLISECONDS);
        GenericRecord avroRecord = createV1Record(v1Schema);
        long ts = extractor.getWatermarkTimeMs(avroRecord);
        Assert.assertEquals(ts, TIMESTAMP_MS);
    }

    @Test
    public void testV1SchemaWithMicroSecond() {
        WatermarkTimeExtractor extractor = new WatermarkTimeExtractor(v1Schema, V1_TS_FIELD_NAME, TimeUnit.MICROSECONDS);
        GenericRecord avroRecord = new GenericData.Record(v1Schema);
        avroRecord.put(DATEINT, 2020);
        avroRecord.put(HOUR, 0);
        avroRecord.put(EVENT_UTC_MS, TimeUnit.MILLISECONDS.toMicros(TIMESTAMP_MS));
        long ts = extractor.getWatermarkTimeMs(avroRecord);
        Assert.assertEquals(ts, TIMESTAMP_MS);
    }

    @Test
    public void testV1Whitespace() {
        WatermarkTimeExtractor extractor = new WatermarkTimeExtractor(
                v1Schema, V1_TS_FIELD_NAME + " ", TimeUnit.MILLISECONDS);
        GenericRecord avroRecord = createV1Record(v1Schema);
        long ts = extractor.getWatermarkTimeMs(avroRecord);
        Assert.assertEquals(ts, TIMESTAMP_MS);
    }

    @Test
    public void testV1WithV1V3Combo() {
        WatermarkTimeExtractor extractor = new WatermarkTimeExtractor(
                v1Schema, V1_TS_FIELD_NAME + "," + V3_TS_FIELD_NAME, TimeUnit.MILLISECONDS);
        GenericRecord avroRecord = createV1Record(v1Schema);
        long ts = extractor.getWatermarkTimeMs(avroRecord);
        Assert.assertEquals(ts, TIMESTAMP_MS);
    }

    @Test
    public void testV1WithV3V1Combo() {
        WatermarkTimeExtractor extractor = new WatermarkTimeExtractor(
                v1Schema, V3_TS_FIELD_NAME + "," + V1_TS_FIELD_NAME, TimeUnit.MILLISECONDS);
        GenericRecord avroRecord = createV1Record(v1Schema);
        long ts = extractor.getWatermarkTimeMs(avroRecord);
        Assert.assertEquals(ts, TIMESTAMP_MS);
    }

    @Test
    public void testV1SchemaWithNullValue() {
        WatermarkTimeExtractor extractor = new WatermarkTimeExtractor(v1Schema, V1_TS_FIELD_NAME, TimeUnit.MILLISECONDS);
        GenericRecord avroRecord = new GenericData.Record(v1Schema);
        avroRecord.put(DATEINT, 2020);
        avroRecord.put(HOUR, 0);
        Long ts = extractor.getWatermarkTimeMs(avroRecord);
        Assert.assertNull(ts);
    }

    @Test
    public void testV3Schema() {
        WatermarkTimeExtractor extractor = new WatermarkTimeExtractor(v3Schema, V3_TS_FIELD_NAME, TimeUnit.MILLISECONDS);
        GenericRecord avroRecord = createV3Record(v3Schema);
        long ts = extractor.getWatermarkTimeMs(avroRecord);
        Assert.assertEquals(ts, TIMESTAMP_MS);
    }

    @Test
    public void testV3SchemaWithV1V3Combo() {
        WatermarkTimeExtractor extractor = new WatermarkTimeExtractor(
                v3Schema, V1_TS_FIELD_NAME + "," + V3_TS_FIELD_NAME, TimeUnit.MILLISECONDS);
        GenericRecord avroRecord = createV3Record(v3Schema);
        long ts = extractor.getWatermarkTimeMs(avroRecord);
        Assert.assertEquals(ts, TIMESTAMP_MS);
    }

    @Test
    public void testV3SchemaWithV3V1Combo() {
        WatermarkTimeExtractor extractor = new WatermarkTimeExtractor(
                v3Schema, V3_TS_FIELD_NAME + "," + V1_TS_FIELD_NAME, TimeUnit.MILLISECONDS);
        GenericRecord avroRecord = createV3Record(v3Schema);
        long ts = extractor.getWatermarkTimeMs(avroRecord);
        Assert.assertEquals(ts, TIMESTAMP_MS);
    }

    @Test
    public void testV3Whitespace() {
        WatermarkTimeExtractor extractor = new WatermarkTimeExtractor(
                v3Schema, " " + V3_TS_FIELD_NAME + " ", TimeUnit.MILLISECONDS);
        GenericRecord avroRecord = createV3Record(v3Schema);
        long ts = extractor.getWatermarkTimeMs(avroRecord);
        Assert.assertEquals(ts, TIMESTAMP_MS);
    }

    @Test
    public void testV3SchemaWithNullStructValue() {
        WatermarkTimeExtractor extractor = new WatermarkTimeExtractor(v3Schema, V3_TS_FIELD_NAME, TimeUnit.MILLISECONDS);
        GenericRecord avroRecord = new GenericData.Record(v3Schema);
        avroRecord.put(DATEINT, 2020);
        avroRecord.put(HOUR, 0);
        Long ts = extractor.getWatermarkTimeMs(avroRecord);
        Assert.assertNull(ts);
    }

    @Test
    public void testV3SchemaWithNullTimestampValue() {
        WatermarkTimeExtractor extractor = new WatermarkTimeExtractor(v3Schema, V3_TS_FIELD_NAME, TimeUnit.MILLISECONDS);
        Schema metadataSchema = AvroUtiils.getActualSchema(v3Schema.getField(METADATA).schema());
        GenericRecord metadataRecord = new GenericData.Record(metadataSchema);
        GenericRecord avroRecord = new GenericData.Record(v3Schema);
        avroRecord.put(DATEINT, 2020);
        avroRecord.put(HOUR, 0);
        avroRecord.put(METADATA, metadataRecord);
        Long ts = extractor.getWatermarkTimeMs(avroRecord);
        Assert.assertNull(ts);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testV1FieldNotDefined() {
        final Types.StructType struct = Types.StructType.of(
                Types.NestedField.optional(1, DATEINT, Types.IntegerType.get()),
                Types.NestedField.optional(2, HOUR, Types.IntegerType.get())
        );
        org.apache.iceberg.Schema icebergSchema = new org.apache.iceberg.Schema(struct.fields());
        Schema badSchema = AvroSchemaUtil.convert(icebergSchema, TABLE_NAME);
        new WatermarkTimeExtractor(badSchema, V1_TS_FIELD_NAME, TimeUnit.MILLISECONDS);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testV1NotLongType() {
        final Types.StructType struct = Types.StructType.of(
                Types.NestedField.optional(1, DATEINT, Types.IntegerType.get()),
                Types.NestedField.optional(2, HOUR, Types.IntegerType.get()),
                Types.NestedField.optional(3, EVENT_UTC_MS, Types.IntegerType.get())
        );
        org.apache.iceberg.Schema icebergSchema = new org.apache.iceberg.Schema(struct.fields());
        Schema badSchema = AvroSchemaUtil.convert(icebergSchema, TABLE_NAME);
        new WatermarkTimeExtractor(badSchema, V1_TS_FIELD_NAME, TimeUnit.MILLISECONDS);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testV3LeafFieldNotDefined() {
        final Types.StructType struct = Types.StructType.of(
                Types.NestedField.optional(1, DATEINT, Types.IntegerType.get()),
                Types.NestedField.optional(2, HOUR, Types.IntegerType.get()),
                Types.NestedField.optional(3, METADATA,
                        Types.StructType.of(
                                Types.NestedField.optional(4, "other_field", Types.LongType.get())
                        ))
        );
        org.apache.iceberg.Schema icebergSchema = new org.apache.iceberg.Schema(struct.fields());
        Schema badSchema = AvroSchemaUtil.convert(icebergSchema, TABLE_NAME);
        new WatermarkTimeExtractor(badSchema, V3_TS_FIELD_NAME, TimeUnit.MILLISECONDS);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testV3UpstreamFieldNotStruct() {
        final Types.StructType struct = Types.StructType.of(
                Types.NestedField.optional(1, DATEINT, Types.IntegerType.get()),
                Types.NestedField.optional(2, HOUR, Types.IntegerType.get()),
                Types.NestedField.optional(3, METADATA, Types.BooleanType.get())
        );
        org.apache.iceberg.Schema icebergSchema = new org.apache.iceberg.Schema(struct.fields());
        Schema badSchema = AvroSchemaUtil.convert(icebergSchema, TABLE_NAME);
        new WatermarkTimeExtractor(badSchema, V3_TS_FIELD_NAME, TimeUnit.MILLISECONDS);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testV3NotLongType() {
        final Types.StructType struct = Types.StructType.of(
                Types.NestedField.optional(1, DATEINT, Types.IntegerType.get()),
                Types.NestedField.optional(2, HOUR, Types.IntegerType.get()),
                Types.NestedField.optional(3, METADATA,
                        Types.StructType.of(
                                Types.NestedField.optional(4, INGESTION_TIMESTAMP_MS, Types.StringType.get())
                        ))
        );
        org.apache.iceberg.Schema icebergSchema = new org.apache.iceberg.Schema(struct.fields());
        Schema badSchema = AvroSchemaUtil.convert(icebergSchema, TABLE_NAME);
        new WatermarkTimeExtractor(badSchema, V3_TS_FIELD_NAME, TimeUnit.MILLISECONDS);
    }
}
