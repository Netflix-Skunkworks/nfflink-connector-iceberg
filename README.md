
Declarations
=============
* Current code is not ready as proper open source implementation,
as it has couplings with Netflix ecosystem (e.g. config, metrics).
* We have spend some effort to make it compile.
But this code doesn't run, 
as  we did some dummy substition just to make it compile.
e.g `MetacatIcebergCatalog` (from Netflix jar) is set to `null` 
with `BaseMetastoreCatalog` type (open source jar).
* Main purpose is to share _an_ implementation of Iceberg sink inside Netflix
as an example. 

Limitations 
=============
* It can't handle multiple concurrent checkpoints,
as such restriction simplifies the commit logic.
It is not a problem for us,
as we always set `execution.checkpointing.max-concurrent-checkpoints=1`.
* Right now it converts input data type to Avro `IndexedRecord` first. 
It then calls `org.apache.iceberg.parquet.Parquet` writer from Iceberg lib 
for writing Avro IndexedRecord to Parquet files.
If input type is Avro `IndexedRecord` already, 
then no type conversion is needed.
We often use `java.util.Map` as input data type to Iceberg sink.
So we provided a `MapAvorSerializer` that converts `Map` to Avro `GenericRecord`.

Design
=============

Iceberg sink is implemented as two-stage operators:
* parallel writers, which is a regular stream operator. 
See `IcebergWriter` class.
* single-parallelism committer, which is actually a sink operator. 
See `IcebergCommitter` class.
* `IcebergSinkAppender` is a util class that
attaches the two-staged Iceberg sink to DataStream.
It sets the parallelism correctly (e.g. for committer).

Single-parallelism committer is running on taskmanager.
You can see some details on why in this doc:
https://docs.google.com/document/d/1O-dPaFct59wUWQECXEEYIkl9_MOoG3zTbC2V-fZRwrg/edit#

Checkpoint logic
* `IcebergWriter#prepareSnapshotPreBarrier` flushes the file 
and complete the upload to S3.
It then send the locations and other metadata (like low and high watermark) 
of uploaded S3 files (see `FlinkDataFile`)
to downstream `IcebergCommitter` operator.
Note that flush is done inside `prepareSnapshotPreBarrier` method of `IcebergWriter`,
which happens before `IcebergWriter` forwards checkpoint barrier to `IcebergCommitter`.
* Once `IcebergCommitter` received checkpoint barrier from all upstream `IcebergWriter` subtasks,
`IcebergCommitter#snapshotState` write the list of received data files 
to a Iceberg manifest file in S3.
It then checkpoints the manifest file to Flink operator list state.
Checkpoint manifest file (rath than the list of data files) 
is an optimization to minimize/reduce the operate state size.
Number of data files could be very large (e.g. thousands),
while manifest file is always one per checkpoint.
Upon the completion of a successful checkpoint,
`IcebergCommitter#notifyCheckpointComplete` commit the manifest file to Iceberg table

Failure handling
* if `IcebergCommitter#notifyCheckpointComplete` failed to commit files to Iceberg table
due to whatever reason (e.g. transient network issue, transient service issue with metacat),
current implementation absorbs the failure and job continues.
There is no data loss, since the manifest file is checkpointed. 
When next checkpoint come, committer will commit the new manifest file 
along with last uncommitted old manifest file.
* If there is any failure that cause job to restart from last completed checkpoint,
any uploaded S3 files are essentially abandoned rightfully.
Job goes back to previous Kafka offsets from last completed checkpoint, 
and re-process events from that point.
There is no data loss or duplicate in this case.

Misc
=============
* Metrics are published using [Spectator metric system](https://github.com/Netflix/spectator)




