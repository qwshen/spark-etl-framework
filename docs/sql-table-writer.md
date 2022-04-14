The SqlTableWriter is for writing a Spark Dataframe into a target table. The writing behavior can be overwrite or append which is specified in the mode property.

When writing data into tables, the following properties can be defined to further control the writing behavior:
- partitionBy: columns separated by comma used to partition the data before writing
- numPartitions: the number of partitions for the dataframe to be partitioned into before writing.

Actor Class: `com.qwshen.etl.sink.SqlTableWriter`

The definition for the SqlTableWriter:

- In YAML format
```yaml
    actor:
      type: sql-table-writer
      properties:
        table: eCommerce.events
        mode: overwrite
        view: events
```

- In JSON format
```json
  {
    "actor": {
      "type": "sql-table-writer",
        "properties": {
          "table": "eCommerce.users",
          "partitionBy": "age, gender",
          "numPartitions": "16",
          "mode": "append",
          "view": "users"
      }
    }
  }
```

- In XML format
```xml
    <actor type="sql-table-reader">
        <properties>
            <table>eCommerce.users</table>
            <partitionBy>gender, age</partitionBy>
            <numPartitions>16</numPartitions>
            <mode>append</mode>
            <view>users</view>
        </properties>
    </actor>
```
