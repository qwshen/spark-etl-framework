The SparkConfActor is for updating Spark configurations at runtime. If the change is on the global Spark-Session, then it will change
the behavior of all actors (from all jobs & actions) in a pipeline. One good practice is to make a data process pipeline into multiple
work-units, and each of them is defined a job in the pipeline (also turn of the singleSparkSession off at the pipeline level), so in 
such case, any change to the Spark-Conf in one job only affects the behavior of the actors associated with the job.

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
    </properties>
  </actor>
```