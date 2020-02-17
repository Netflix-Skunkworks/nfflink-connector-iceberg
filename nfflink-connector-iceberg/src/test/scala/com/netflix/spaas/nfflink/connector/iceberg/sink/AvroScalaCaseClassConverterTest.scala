package com.netflix.spaas.nfflink.connector.iceberg.sink

import java.io.File
import java.util.UUID

import org.apache.avro.Schema
import org.testng.Assert
import org.testng.annotations.Test

class AvroScalaCaseClassConverterTest {

  @Test
  @throws[Exception]
  def foo(): Unit = {
    val schemaFile = new File(getClass.getClassLoader.getResource("foo.avsc").getFile)
    val avroSchema = new Schema.Parser().parse(schemaFile)
    val serializer = new PojoAvroSerializer[FooCase]()
    val pojo = FooCase(UUID.randomUUID.toString, System.currentTimeMillis * 1000L)
    val genericRecord = serializer.serialize(pojo, avroSchema)
    System.out.println("uuid = " + genericRecord.get("uuid") + ", ts = " + genericRecord.get("ts"))
    Assert.assertEquals(pojo.uuid, genericRecord.get("uuid").toString);
    Assert.assertEquals(pojo.ts, genericRecord.get("ts"))
  }

}
