The SparkConfActor is for updating Spark configurations at runtime. If the change is on the global Spark-Session, then it will change
the behavior of all actors (from all jobs & actions) in a pipeline. One good practice is to make a data process pipeline into multiple
work-units, and each of them is defined as a job in the pipeline (also turn off the singleSparkSession at the pipeline level), so in 
such case, any changes to the Spark-Conf in one job only affect the behavior of the actors associated with the job.

- the configs property is for specifying key-value pairs for the Spark configurations
- the hadoopConfigs property is for specifying key-value pairs for the Hadoop configurations of the Spark-Context.
 
Actor Class: `com.qwshen.etl.common.SparkConfActor`

The definition of the SparkConfActor:
- in YAML
```yaml
  actor:
    type: com.qwshen.etl.common.SparkConfActor
    properties:
      configs:
        spark.sql.shuffle.partitions: 300
        spark.files.overwrite: true
        spark.sql.adaptive.enabled: true
      hadoopConfigs:
        mapreduce.fileoutputcommitter.marksuccessfuljobs: false
```
- in JSON
```json
  {
    "actor": {
      "type": "com.qwshen.etl.common.SparkConfActor",
      "properties": {
        "configs": {
          "spark.sql.shuffle.partitions": 300,
          "spark.files.overwrite": true,
          "spark.sql.adaptive.enabled": true
        },
        "hadoopConfigs": {
          "mapreduce.fileoutputcommitter.marksuccessfuljobs": false
        }
      }
    }
  }
```
- in XML
```xml
  <actor type="com.qwshen.etl.common.SparkConfActor">
    <properties>
      <configs>
        <spark.sql.shuffle.partitions>300</spark.sql.shuffle.partitions>
        <spark.files.overwrite>true</spark.files.overwrite>
        <spark.sql.adaptive.enabled>true</spark.sql.adaptive.enabled>
      </configs>
      <hadoopConfigs>
        <mapreduce.fileoutputcommitter.marksuccessfuljobs>false</mapreduce.fileoutputcommitter.marksuccessfuljobs>
      </hadoopConfigs>
    </properties>
  </actor>
```
