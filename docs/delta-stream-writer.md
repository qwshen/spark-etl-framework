The DeltaStreamWriter is for writing a data-frame to delta lake in streaming mode.

- The supported write-options are as follows
    - replaceWhere - to specify the target data to be replaced
    - userMeta - to add user-defined metadata in delta table commits
    - overwriteSchema - to overwrite the existing target schema by changing a column's type or name or dropping a column, thus it requires rewriting the target (table). So this normally is used with overwrite mode.
    - mergeSchema - to merge the source schema into the target table.
    - checkpointLocation - the location for writing streaming checkpoints.
- The partition-by is optional. If provided, it must be the names of one or more columns separated by comma.
- The trigger mode must be one of the following values:
  - continuous
  - processingTime
  - once
- The output mode must be one of the following values:
  - complete
  - append
  - update
- The test.waittimeMS is for testing purpose which specify how long the streaming run will be last.
- The location of the writing can be either by sinkPath or sinkTable. If both specified, sinkTable takes precedence.

Actor Class: `com.qwshen.etl.sink.DeltaStreamWriter`

The definition of the DeltaStreamWriter:

- In YAML format
```yaml
  actor:
    type: delta-writer
    properties:
      options:
        replaceWhere: "date >= '2020-05-21' and date < '2020-06-30'"
        userMeta: "replace to fix incorrect data"
        mergeSchema: true
      partitionBy: "joined_at, gender"
      bucket:
        numBuckets: 16
        by: user_id
      mode: overwrite
      trigger:
        mode: continuous
        interval: 3 seconds
      outputMode: append
      test.waittimeMS: 30000
      sinkPath: /tmp/users
      view: users      
```
- In JSON format
```json
  {
    "actor": {
      "type": "delta-writer",
      "properties": {
        "options": {
          "replaceWhere": "date >= '2020-05-21' and date < '2020-06-30'",
          "userMeta": "replace to fix incorrect data",
          "mergeSchema": true
        },
        "partitionBy": "joined_at, gender",
        "bucket": {
          "numBuckets": 16,
          "by": "user_id"
        },
        "mode": "overwrite",
        "trigger": {
          "mode": "continuous",
          "interval": "3 seconds"
        },
        "outputMode": "append",
        "test.waittimeMS": "30000",
        "sinkTable": "users",
        "view": "users"
      }
    }
  }
```
- In XML format
```xml
  <actor type="delta-writer">
    <properties>
      <options>
        <replaceWhere>date >= '2020-05-21' and date &lt; '2020-06-30'</replaceWhere>
        <userMeta>replace to fix incorrect data</userMeta>
        <mergeSchema>true</mergeSchema>
      </options>
      <partitionBy>joined_at, gender</partitionBy>
      <bucket>
        <numBuckets>16</numBuckets>
        <by>user_id</by>
      </bucket>
      <mode>overwrite</mode>
      <trigger>
        <mode>continuous</mode>
        <interval>5 seconds</interval>
      </trigger>
      <outputMode>append</outputMode>
      <test.waittimeMS>30000</test.waittimeMS>
      <sinkTable>users</sinkTable>
      <view>users</view>
    </properties>
  </actor>
```
