package com.netflix.spaas.nfflink.connector.iceberg.sink;

import com.netflix.spaas.nfflink.connector.iceberg.model.StudentUtil;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MapAvroSerializerTest {

    @Test
    public void testStudent() throws Exception {
        final File schemaFile = new File(getClass().getClassLoader().getResource("student.avsc").getFile());
        final Schema avroSchema = new Schema.Parser().parse(schemaFile);

        final Map<String, Object> studentMap = StudentUtil.createStudentMap();
        final MapAvroSerializer serializer = new MapAvroSerializer();
        final GenericRecord genericRecord = serializer.serialize(studentMap, avroSchema);
        Assert.assertTrue(StudentUtil.isEqual(studentMap, genericRecord, avroSchema));
        Assert.assertNull(genericRecord.get("alias"));
    }

    @Test
    public void testOptionalFieldsSet() throws Exception {
        final File schemaFile = new File(getClass().getClassLoader().getResource("student.avsc").getFile());
        final Schema avroSchema = new Schema.Parser().parse(schemaFile);

        final Map<String, Object> studentMap = new HashMap<>(StudentUtil.createStudentMap());
        final List<String> classesList = Arrays.asList("c1", null, "c2");
        studentMap.put("classes", classesList);
        final String alias = "alias_name";
        studentMap.put("alias", alias);

        final MapAvroSerializer serializer = new MapAvroSerializer();
        final GenericRecord genericRecord = serializer.serialize(studentMap, avroSchema);
        Assert.assertTrue(StudentUtil.isEqual(studentMap, genericRecord, avroSchema));
        Assert.assertNotNull(genericRecord.get("classes"));
        List<String> classesFromGenericRecord = ((List<CharSequence>) genericRecord.get("classes"))
                .stream().map(e -> null == e ? null : e.toString()).collect(Collectors.toList());
        Assert.assertEquals(classesFromGenericRecord, classesList);
        Assert.assertNotNull(genericRecord.get("alias"));
        Assert.assertEquals(genericRecord.get("alias").toString(), alias);
    }
    
    @Test(expectedExceptions = AvroTypeException.class)
    public void failWithNullArrayElement() throws Exception {
        final File schemaFile = new File(getClass().getClassLoader().getResource("student.avsc").getFile());
        final Schema avroSchema = new Schema.Parser().parse(schemaFile);

        final Map<String, Object> studentMap = new HashMap<>(StudentUtil.createStudentMap());
        final List<Integer> scores = Arrays.asList(1, null, 3);
        studentMap.put("scores", scores);

        final MapAvroSerializer serializer = new MapAvroSerializer();
        final GenericRecord genericRecord = serializer.serialize(studentMap, avroSchema);
    }

    @Test(expectedExceptions = AvroTypeException.class)
    public void failWithUnionIntStr() throws Exception {
        final File schemaFile = new File(getClass().getClassLoader().getResource("unionIntStr.avsc").getFile());
        final Schema avroSchema = new Schema.Parser().parse(schemaFile);
        final Map<String, Object> mapWithUnionField = new HashMap<>();
        mapWithUnionField.put("unionIntStr", 1);

        final MapAvroSerializer serializer = new MapAvroSerializer();
        final GenericRecord genericRecord = serializer.serialize(mapWithUnionField, avroSchema);
    }
}
