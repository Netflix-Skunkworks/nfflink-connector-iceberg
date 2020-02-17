package com.netflix.spaas.nfflink.connector.iceberg.sink;

import com.netflix.spaas.nfflink.connector.iceberg.model.Student;
import com.netflix.spaas.nfflink.connector.iceberg.model.StudentUtil;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.File;
import java.util.UUID;

public class PojoAvroSerializerTest {

    @Test
    public void foo() throws Exception {
        File schemaFile = new File(getClass().getClassLoader().getResource("foo.avsc").getFile());
        Schema avroSchema = new Schema.Parser().parse(schemaFile);
        PojoAvroSerializer serializer = new PojoAvroSerializer();

        for (int i = 0; i < 3; ++i) {
            Foo pojo = new Foo();
            pojo.setUuid(UUID.randomUUID().toString());
            pojo.setTs(System.currentTimeMillis() * 1000L);
            GenericRecord genericRecord = serializer.serialize(pojo, avroSchema);
            Assert.assertEquals(pojo.getUuid(), genericRecord.get("uuid").toString());
            Assert.assertEquals(pojo.getTs(), genericRecord.get("ts"));
        }
    }

    @Test
    public void testStudent() throws Exception {
        File schemaFile = new File(getClass().getClassLoader().getResource("student.avsc").getFile());
        Schema avroSchema = new Schema.Parser().parse(schemaFile);
        PojoAvroSerializer serializer = new PojoAvroSerializer();

        Student student = StudentUtil.createStudent();
        student.setAlias("alias_name");
        GenericRecord genericRecord = serializer.serialize(student, avroSchema);
        Assert.assertTrue(StudentUtil.isEqual(student, genericRecord));
        Assert.assertNotNull(genericRecord.get("alias"));
        Assert.assertEquals(genericRecord.get("alias").toString(), "alias_name");

        student.setAlias(null);
        genericRecord = serializer.serialize(student, avroSchema);
        Assert.assertTrue(StudentUtil.isEqual(student, genericRecord));
        Assert.assertNull(genericRecord.get("alias"));
    }
}
