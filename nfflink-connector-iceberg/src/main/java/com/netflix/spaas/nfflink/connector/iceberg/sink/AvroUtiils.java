package com.netflix.spaas.nfflink.connector.iceberg.sink;

import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;

import java.util.List;

public class AvroUtiils {
    /**
     * we only support union type for optional field.
     * we don't support union for arbitrary types, e.g. union of int and string.
     * */
    public static Schema getActualSchema(Schema fieldSchema) {
        Schema actualSchema = null;
        if (fieldSchema.getType().equals(Schema.Type.UNION)) {
            List<Schema> schemaList = fieldSchema.getTypes();
            // this should only contain two entries, the actual type and NULL
            for (Schema schema : schemaList) {
                if (!schema.getType().equals(Schema.Type.NULL)) {
                    if (null == actualSchema) {
                        actualSchema = schema;
                    } else {
                        throw new AvroTypeException("Only support union for optional/nullable field: " + fieldSchema.getName());
                    }
                }
            }
        } else {
            actualSchema = fieldSchema;
        }
        if (actualSchema == null) {
            throw new AvroTypeException("No actual schema for field: " + fieldSchema.getName());
        }
        return actualSchema;
    }

    public static boolean isOptional(Schema fieldSchema) {
        boolean isOptional = false;
        if (fieldSchema.getType().equals(Schema.Type.UNION)) {
            for (Schema schema : fieldSchema.getTypes()) {
                if (schema.getType().equals(Schema.Type.NULL)) {
                    isOptional = true;
                }
            }
        }
        return isOptional;
    }
}
