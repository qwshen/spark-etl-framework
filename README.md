1. To pass the Kafka unit-tests, please set up a Kafka environment by following the steps in kafka_setup.sh

- [SqlTransformer](docs/sql-transformer.md)
- [FileReader](docs/file-reader.md)
- [FlatReader](docs/flat-reader.md)
- [KafkaReader](docs/kafka-reader.md)
- [KafkaStreamReader](docs/kafka-stream-reader.md)
- [FileWriter](docs/file-writer.md)
- [KafkaWriter](docs/kafka-writer.md)
- [KafkaStreamWriter](docs/kafka-stream-writer.md)





TODO:
- check if ObjectCreator is required.
- delete propertyParser.scala
- in PropertyComponent, remove method - init(definition: NodeSeq, config: Config)(implicit session: SparkSession): Unit

Note:

HiveWriter supports bucket-by, please see
https://stackoverflow.com/questions/52799025/error-using-spark-save-does-not-support-bucketing-right-now