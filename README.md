Spark-etl-framework is a pipeline based data transformation framework using Spark-SQL. For one process to transform & move data from end to end, a pipeline needs to be defined,
and as a start of a pipeline, one or more readers is/are required to load data from the source(s), then data gets transformed in the middle by using Spark-SQL (extensible 
with custom components), finally at the end (not necessarily) of the pipeline, writer(s) write(s) out the result to target storage.

A pipeline consists of multiple jobs, and each job contains multiple actions (each is represented by an Actor). Each job can run under the same or separate Spark
(sub) Session, and dataframes (as views) can be shared across jobs. Each action (except configuration update) requires one or more input views and produces one 
output view. 

To build the project
```
  mvn clean install -DskipTests
```
[Go through unit tests](docs/unit-tests.md)

### Pipeline definition
The following explains the definition of each section in a pipeline:

Pipeline Examples

### Configuration

### Submitting a spark-job



### Source readers
- [FileReader](docs/file-reader.md)
- [FileStreamReader](docs/file-stream-reader.md)
- [FlatReader](docs/flat-reader.md)
- [FlatStreamReader](docs/flat-stream-reader.md)
- [KafkaReader](docs/kafka-reader.md)
- [KafkaStreamReader](docs/kafka-stream-reader.md)
- [DeltaReader](docs/delta-reader.md)
- [DeltaStreamReader](docs/delta-stream-reader.md)
- [JdbcReader](docs/jdbs-reader.md)
- [HBaseReader](docs/hbase-reader.md)

### Transformers
- [SqlTransformer](docs/sql-transformer.md)

### Sink writers
- [FileWriter](docs/file-writer.md)
- [FileStreamWriter](docs/file-stream-writer.md)
- [KafkaWriter](docs/kafka-writer.md)
- [KafkaStreamWriter](docs/kafka-stream-writer.md)
- [DeltaWriter](docs/delta-writer.md)
- [DeltaStreamWriter](docs/delta-stream-writer.md)
- [JdbcWriter](docs/jdbc-writer.md)
- [JdbcStreamWriter](docs/jdbc-stream-writer.md)
- [HBaseWriter](docs/hbase-writer.md)
- [HBaseStremWriter](docs/hbase-stream-writer.md)
- 
### Writing custom Actor

### Spark-SQL practices
