

TODO:
- check if ObjectCreator is required.
- delete propertyParser.scala
- in PropertyComponent, remove method - init(definition: NodeSeq, config: Config)(implicit session: SparkSession): Unit

Note:

HiveWriter supports bucket-by, please see
https://stackoverflow.com/questions/52799025/error-using-spark-save-does-not-support-bucketing-right-now