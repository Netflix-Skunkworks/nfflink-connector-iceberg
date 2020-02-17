package com.netflix.spaas.nfflink.connector.iceberg.sink;

import org.apache.avro.AvroTypeException;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.types.Types;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class MapAvroCompatibilityTest {

    private static final String OTHER_PROPERTIES = "other_properties";
    private static final String EVENT_UTC_MS = "event_utc_ms";
    private static final String HOSTNAME = "hostname";
    private static final String RESTRICTED_DO_NOT_COPY = "restricted_do_not_copy";
    private static final String DATEINT = "dateint";
    private static final String HOUR = "hour";
    private static final String BATCHID = "batchid";
    private static final String CAPP = "capp";
    private static final String ROW_ID = "row_id";

    private static final String C_STR = "c_str";
    private static final String C_INT = "c_int";
    private static final String C_LONG = "c_long";
    private static final String C_FLOAT = "c_float";
    private static final String C_DOUBLE = "c_double";
    private static final String C_BOOLEAN = "c_boolean";

    private static final String EXTRA_C_STR = "extra_c_str";

    private Map<String, Object> createDataMap() {
        Map<String, Object> m = new HashMap<>();
        final long now = System.currentTimeMillis();
        m.put(EVENT_UTC_MS, now);
        m.put(HOSTNAME, "some_host");
        m.put(RESTRICTED_DO_NOT_COPY, Collections.emptyMap());
        final Pair<Integer, Integer> dateintHourPair = getDateintAndHour(now);
        m.put(DATEINT, dateintHourPair.getLeft());
        m.put(HOUR, dateintHourPair.getRight());
        m.put(BATCHID, "some_batch_id");
        m.put(CAPP, "some_app");
        m.put(ROW_ID, "some_row_id");

        m.put(C_STR, "s");
        m.put(C_INT, Integer.MAX_VALUE );
        m.put(C_LONG, Long.MAX_VALUE);
        m.put(C_FLOAT, Float.MAX_VALUE);
        m.put(C_DOUBLE, Double.MAX_VALUE);
        m.put(C_BOOLEAN, Boolean.TRUE);
        return m;
    }

    private Pair<Integer, Integer> getDateintAndHour(long ms) {
        DateTime dt = new DateTime(ms, DateTimeZone.UTC);
        String dateStr = dt.toString("YYYYMMdd");
        int dateint = Integer.parseInt(dateStr);
        return new ImmutablePair<>(dateint, dt.getHourOfDay());
    }

    private static Schema createIcebergSchema() {
        final Types.StructType struct = Types.StructType.of(
                Types.NestedField.optional(1, OTHER_PROPERTIES,
                        Types.MapType.ofOptional(10, 11,
                                Types.StringType.get(), Types.StringType.get())),
                Types.NestedField.optional(2, EVENT_UTC_MS, Types.LongType.get()),
                Types.NestedField.optional(3, HOSTNAME, Types.StringType.get()),
                Types.NestedField.optional(4, RESTRICTED_DO_NOT_COPY,
                        Types.MapType.ofOptional(12, 13,
                                Types.StringType.get(), Types.StringType.get())),
                Types.NestedField.optional(5, DATEINT, Types.IntegerType.get()),
                Types.NestedField.optional(6, HOUR, Types.IntegerType.get()),
                Types.NestedField.optional(7, BATCHID, Types.StringType.get()),
                Types.NestedField.optional(8, CAPP, Types.StringType.get()),
                Types.NestedField.optional(9, ROW_ID, Types.StringType.get()),

                Types.NestedField.required(14, C_STR, Types.StringType.get()),
                Types.NestedField.optional(15, C_LONG, Types.LongType.get()),
                Types.NestedField.optional(16, C_INT, Types.IntegerType.get()),
                Types.NestedField.optional(17, C_DOUBLE, Types.DoubleType.get()),
                Types.NestedField.optional(18, C_FLOAT, Types.FloatType.get()),
                Types.NestedField.optional(19, C_BOOLEAN, Types.BooleanType.get())
        );
        return new Schema(struct.fields());
    }

    private static final String tableName = MapAvroCompatibilityTest.class.getSimpleName();
    private static Schema icebergSchema = createIcebergSchema();
    private static org.apache.avro.Schema avroSchema = AvroSchemaUtil.convert(icebergSchema, tableName);
    private static MapAvroSerializer avroSerializer = MapAvroSerializer.getInstance();

    @BeforeClass
    public static void setUp() throws Exception {
    }

    @AfterClass
    public static void tearDown() throws Exception {
    }

    @Test
    public void testExactlyMatch() throws Exception {
        final Map<String, Object> m = createDataMap();
        final GenericRecord genericRecord = avroSerializer.serialize(m, avroSchema);
    }

    /**
     * MapAvroSerializer implementation ignore fields not defined in schema
     * when converting java.util.Map to GenericRecord.
     */
    @Test
    public void testIgnoreExtraColumn() throws Exception {
        final Map<String, Object> m = createDataMap();
        m.put(EXTRA_C_STR, "extra_c_some_str");
        final GenericRecord genericRecord = avroSerializer.serialize(m, avroSchema);
        Assert.assertNull(genericRecord.get(EXTRA_C_STR));
    }

    @Test(expectedExceptions = AvroTypeException.class)
    public void testLongToInt() throws Exception {
        final Map<String, Object> m = createDataMap();
        final Long n = Long.MAX_VALUE;
        m.put(C_INT, n);
        final GenericRecord genericRecord = avroSerializer.serialize(m, avroSchema);
        Assert.assertEquals(n.intValue(), genericRecord.get(C_INT));
    }

    @Test(expectedExceptions = AvroTypeException.class)
    public void testFloatToInt() throws Exception {
        final Map<String, Object> m = createDataMap();
        final Float n = Float.MAX_VALUE;
        m.put(C_INT, n);
        final GenericRecord genericRecord = avroSerializer.serialize(m, avroSchema);
        Assert.assertEquals(n.intValue(), genericRecord.get(C_INT));
    }

    @Test(expectedExceptions = AvroTypeException.class)
    public void testDoubleToInt() throws Exception {
        final Map<String, Object> m = createDataMap();
        final Double n = Double.MAX_VALUE;
        m.put(C_INT, n);
        final GenericRecord genericRecord = avroSerializer.serialize(m, avroSchema);
        Assert.assertEquals(n.intValue(), genericRecord.get(C_INT));
    }

    @Test(expectedExceptions = AvroTypeException.class)
    public void testStringToInt() throws Exception {
        final Map<String, Object> m = createDataMap();
        m.put(C_INT, "some_str");
        final GenericRecord genericRecord = avroSerializer.serialize(m, avroSchema);
    }

    @Test
    public void testIntToLong() throws Exception {
        final Map<String, Object> m = createDataMap();
        final Integer n = Integer.MAX_VALUE;
        m.put(C_LONG, n);
        final GenericRecord genericRecord = avroSerializer.serialize(m, avroSchema);
        Assert.assertEquals(n.longValue(), genericRecord.get(C_LONG));
    }

    @Test(expectedExceptions = AvroTypeException.class)
    public void testDoubleToLong() throws Exception {
        final Map<String, Object> m = createDataMap();
        final Double n = Double.MAX_VALUE;
        m.put(C_LONG, n);
        final GenericRecord genericRecord = avroSerializer.serialize(m, avroSchema);
        Assert.assertEquals(n.longValue(), genericRecord.get(C_LONG));
    }

    @Test(expectedExceptions = AvroTypeException.class)
    public void testFloatToLong() throws Exception {
        final Map<String, Object> m = createDataMap();
        final Float n = Float.MAX_VALUE;
        m.put(C_LONG, n);
        final GenericRecord genericRecord = avroSerializer.serialize(m, avroSchema);
        Assert.assertEquals(n.longValue(), genericRecord.get(C_LONG));
    }

    @Test(expectedExceptions = AvroTypeException.class)
    public void testStringToLong() throws Exception {
        final Map<String, Object> m = createDataMap();
        m.put(C_LONG, "some_str");
        final GenericRecord genericRecord = avroSerializer.serialize(m, avroSchema);
    }

    @Test
    public void testIntToFloat() throws Exception {
        final Map<String, Object> m = createDataMap();
        final Integer n = Integer.MAX_VALUE;
        m.put(C_FLOAT, n);
        final GenericRecord genericRecord = avroSerializer.serialize(m, avroSchema);
        Assert.assertEquals(n.floatValue(), genericRecord.get(C_FLOAT));
    }

    @Test
    public void testLongToFloat() throws Exception {
        final Map<String, Object> m = createDataMap();
        final Long n = Long.MAX_VALUE;
        m.put(C_FLOAT, n);
        final GenericRecord genericRecord = avroSerializer.serialize(m, avroSchema);
        Assert.assertEquals(n.floatValue(), genericRecord.get(C_FLOAT));
    }

    @Test(expectedExceptions = AvroTypeException.class)
    public void testDoubleToFloat() throws Exception {
        final Map<String, Object> m = createDataMap();
        final Double n = Double.MAX_VALUE;
        m.put(C_FLOAT, n);
        final GenericRecord genericRecord = avroSerializer.serialize(m, avroSchema);
        Assert.assertEquals(n.floatValue(), genericRecord.get(C_FLOAT));
    }

    @Test(expectedExceptions = AvroTypeException.class)
    public void testStringToFloat() throws Exception {
        final Map<String, Object> m = createDataMap();
        m.put(C_FLOAT, "some_str");
        final GenericRecord genericRecord = avroSerializer.serialize(m, avroSchema);
    }

    @Test
    public void testIntToDouble() throws Exception {
        final Map<String, Object> m = createDataMap();
        final Integer n = Integer.MAX_VALUE;
        m.put(C_DOUBLE, n);
        final GenericRecord genericRecord = avroSerializer.serialize(m, avroSchema);
        Assert.assertEquals(n.doubleValue(), genericRecord.get(C_DOUBLE));
    }

    @Test
    public void testLongToDouble() throws Exception {
        final Map<String, Object> m = createDataMap();
        final Long n = Long.MAX_VALUE;
        m.put(C_DOUBLE, n);
        final GenericRecord genericRecord = avroSerializer.serialize(m, avroSchema);
        Assert.assertEquals(n.doubleValue(), genericRecord.get(C_DOUBLE));
    }

    @Test
    public void testFloatToDouble() throws Exception {
        final Map<String, Object> m = createDataMap();
        final Float n = Float.MAX_VALUE;
        m.put(C_DOUBLE, n);
        final GenericRecord genericRecord = avroSerializer.serialize(m, avroSchema);
        Assert.assertEquals(n.doubleValue(), genericRecord.get(C_DOUBLE));
    }

    @Test(expectedExceptions = AvroTypeException.class)
    public void testStringToDouble() throws Exception {
        final Map<String, Object> m = createDataMap();
        m.put(C_DOUBLE, "some_str");
        final GenericRecord genericRecord = avroSerializer.serialize(m, avroSchema);
    }

    @Test(expectedExceptions = AvroTypeException.class)
    public void testStringToBoolean() throws Exception {
        final Map<String, Object> m = createDataMap();
        m.put(C_BOOLEAN, "some_str");
        final GenericRecord genericRecord = avroSerializer.serialize(m, avroSchema);
    }

    @Test(expectedExceptions = AvroTypeException.class)
    public void testIntToString() throws Exception {
        final Map<String, Object> m = createDataMap();
        final int i = 1;
        m.put(C_STR, Integer.valueOf(i));
        final GenericRecord genericRecord = avroSerializer.serialize(m, avroSchema);
        Assert.assertEquals(new Utf8(Integer.toString(i)), genericRecord.get(C_STR));
    }

    @Test(expectedExceptions = AvroTypeException.class)
    public void testMissingRequiredField() throws Exception {
        final Map<String, Object> m = createDataMap();
        m.remove(C_STR);
        final GenericRecord genericRecord = avroSerializer.serialize(m, avroSchema);
    }

    @Test(expectedExceptions = AvroTypeException.class)
    public void testNullRequiredField() throws Exception {
        final Map<String, Object> m = createDataMap();
        m.put(C_STR, null);
        final GenericRecord genericRecord = avroSerializer.serialize(m, avroSchema);
    }
}
