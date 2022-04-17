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

Actor Class: `com.qwshen.etl.sink.DeltaWriter`

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

Note:
The DeltaWriter provides only fundamental write operations. It is recommended to use SQL commands with SqlWriter for more complicated writes.

1. INSERT INTO (for Spark 3.0 or later)
```
  INSERT INTO default.people10m VALUES (1, 'a'), (2, 'b');

  INSERT INTO default.people10m SELECT ...;
```

2. MERGE INTO (for Spark 3.0 or later)
```
  SET spark.databricks.delta.commitInfo.userMetadata=overwritten-for-fixing-incorrect-data;
  
  MERGE INTO delta.`/tmp/delta/events` target
  USING my_table_yesterday source
    ON source.userId = target.userId
  WHEN MATCHED THEN UPDATE SET *
```

3. INSERT OVERWRITE (make sure Spark is 3.0.1 or later)
```
  INSERT OVERWRITE TABLE default.logs
  SELECT 
    uuid, first(level), first(ts), first(message)
  FROM prod.my_app.logs
  WHERE cast(ts as date) = '2020-07-01'
  GROUP BY uuid
```

4. DELETE FROM (for Spark 3.0 or later)
    ```
    DELETE FROM prod.db.table
    WHERE ts >= '2020-05-01 00:00:00' and ts < '2020-06-01 00:00:00'

    DELETE FROM prod.db.all_events
    WHERE session_time < (SELECT min(session_time) FROM prod.db.good_events)

    DELETE FROM prod.db.orders AS t1
    WHERE EXISTS (SELECT oid FROM prod.db.returned_orders WHERE t1.oid = oid)
    ```
  5. UPDATE (for Spark 3.1 or later)
    ```
    UPDATE prod.db.table
    SET c1 = 'update_c1', c2 = 'update_c2'
    WHERE ts >= '2020-05-01 00:00:00' and ts < '2020-06-01 00:00:00'

    UPDATE prod.db.all_events
    SET session_time = 0, ignored = true
    WHERE session_time < (SELECT min(session_time) FROM prod.db.good_events)

    UPDATE prod.db.orders AS t1
    SET order_status = 'returned'
    WHERE EXISTS (SELECT oid FROM prod.db.returned_orders WHERE t1.oid = oid)
    ```


