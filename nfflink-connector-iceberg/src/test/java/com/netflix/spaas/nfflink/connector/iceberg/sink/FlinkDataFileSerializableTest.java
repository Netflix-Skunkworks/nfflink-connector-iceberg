package com.netflix.spaas.nfflink.connector.iceberg.sink;

import com.google.common.collect.ImmutableMap;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

public class FlinkDataFileSerializableTest {

    @Test
    public void test() throws Exception {
        List<Types.NestedField> columns = Arrays.asList(
                Types.NestedField.required(1, "dateint", new Types.IntegerType()),
                Types.NestedField.required(2, "uuid", new Types.StringType())
        );
        Schema icebergSchema = new Schema(columns);
        PartitionSpec spec = PartitionSpec.builderFor(icebergSchema)
                .identity("dateint")
                .build();
        Metrics metrics = new Metrics(2L,
                ImmutableMap.of(1, 1L, 2, 2L),
                ImmutableMap.of(1, 11L, 2, 22L),
                ImmutableMap.of(1, 111L, 2, 222L),
                ImmutableMap.of(1, ByteBuffer.allocate(16), 2, ByteBuffer.allocate(16)),
                ImmutableMap.of(1, ByteBuffer.allocate(16), 2, ByteBuffer.allocate(16)));
        DataFile dataFile = DataFiles.builder(spec)
                .withPath("s3://bucket/object.parquet")
                .withFormat(FileFormat.PARQUET.name())
                .withFileSizeInBytes(128)
                .withRecordCount(2)
                .withMetrics(metrics)
                .build();
        FlinkDataFile outObj = new FlinkDataFile(1, 2, dataFile);

        final ByteArrayOutputStream baos = new ByteArrayOutputStream(1024 * 1024);
        final ObjectOutputStream os = new ObjectOutputStream(baos);
        os.writeObject(outObj);
        os.close();

        final ObjectInputStream is = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()));
        final FlinkDataFile inObj = (FlinkDataFile) is.readObject();

        final ByteArrayOutputStream baos2 = new ByteArrayOutputStream(1024 * 1024);
        final ObjectOutputStream os2 = new ObjectOutputStream(baos2);
        final FlinkDataFile outObj2 = inObj;
        // this is where it will fail
        os2.writeObject(outObj2);
        os2.close();

        final ObjectInputStream is2 = new ObjectInputStream(new ByteArrayInputStream(baos2.toByteArray()));
        final FlinkDataFile inObj2 = (FlinkDataFile) is2.readObject();
    }
}
