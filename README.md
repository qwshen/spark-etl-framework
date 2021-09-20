1. To pass the Kafka unit-tests, please set up a Kafka environment by following the steps in kafka_setup.sh

### Source Readers
- [FileReader](docs/file-reader.md)
- [FlatReader](docs/flat-reader.md)
- [KafkaReader](docs/kafka-reader.md)
- [KafkaStreamReader](docs/kafka-stream-reader.md)
- [DeltaReader](docs/delta-reader.md)
- [DeltaStreamReader](docs/delta-stream-reader.md)

### Transformers
- [SqlTransformer](docs/sql-transformer.md)

### Sink Writers
- [FileWriter](docs/file-writer.md)
- [KafkaWriter](docs/kafka-writer.md)
- [KafkaStreamWriter](docs/kafka-stream-writer.md)
- [DeltaWriter](docs/delta-writer.md)
- [DeltaStreamWriter](docs/delta-stream-writer.md)




TODO:
- check if ObjectCreator is required.
- delete propertyParser.scala
- in PropertyComponent, remove method - init(definition: NodeSeq, config: Config)(implicit session: SparkSession): Unit

Note:

HiveWriter supports bucket-by, please see
https://stackoverflow.com/questions/52799025/error-using-spark-save-does-not-support-bucketing-right-now