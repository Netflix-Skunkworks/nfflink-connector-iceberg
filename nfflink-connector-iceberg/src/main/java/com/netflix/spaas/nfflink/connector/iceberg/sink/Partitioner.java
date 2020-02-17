package com.netflix.spaas.nfflink.connector.iceberg.sink;

import org.apache.iceberg.StructLike;

interface Partitioner<T> extends StructLike {

    Partitioner<T> copy();

    String toPath();

    void partition(T record);
}
