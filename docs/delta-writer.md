The DeltaWriter is for writing a data-frame to delta lake in batch mode.

- The supported write-options are as follows
  - replaceWhere - to specify the target data to be replaced
  - userMeta - to add user-defined metadata in delta table commits
  - overwriteSchema - to overwrite the existing target schema by changing a column's type or name or dropping a column, thus it requires rewriting the target (table). So this normally is used with overwrite mode.
  - mergeSchema - to merge the source schema into the target table.
- The write mode can only be overwrite or append
- The partition-by is optional. If provided, it must be the names of one or more columns separated by comma.
- The bucket is to split the source data by the columns into # of buckets specified by numBucket. The by field is the name of one or more columns separated by comma. Please note, if bucketing is intended, both numBuckets & by fields must be provided.
  - Important: bucketing only supports saveAsTable for the time being
- The location of the writing can be either by sinkPath or sinkTable. If both specified, sinkTable takes precedence.

The definition of the DeltaWriter:

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
      <sinkTable>users</sinkTable>
      <view>users</view>
    </properties>
  </actor>
```