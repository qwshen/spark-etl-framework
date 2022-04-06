The ViewPartitioner is for partitioning an existing view (dataframe) into the number of partitions 
by the optional columns, which are separated by comma.

Actor Class: `com.qwshen.etl.utils.ViewPartitioner`

The definition of the ViewPartitioner:
- in YAML
```yaml
  actor:
    type: view-partitioner
    properties:
      numPartitions: 160
      partitionBy: gender,age
      view: users
```
- in JSON
```json
  {
    "actor": {
      "type": "view-partitioner",
      "properties": {
        "numPartitions": "160",
        "partitionBy": "gender"
      }
    }
  }
```
- in XML
```xml
  <actor type="view-partitioner">
    <properties>
      <numPartitions>160</numPartitions>
    </properties>
  </actor>
```
