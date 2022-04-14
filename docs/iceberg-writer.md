The IcebergWriter is for writing data-frames to iceberg tables in batch mode with DataFrame API.

- The location of the writing can be either an existing iceberg table or a directory path.
- The supported write-options are as follows
  - write-format - specify the file format to use for the write operation; It should be one of parquet, avro, or orc etc. Default: write.format.default - table property when the table was being created.
  - target-file-size-bytes - override the target table’s write.target-file-size-bytes. Default: as per table property.
  - check-nullability - set the nullable check on fields. Default: true.
  - snapshot-property.custom-key - add an entry with custom-key and corresponding value in the snapshot summary. Default: null.
  - fanout-enabled - override the target table’s write.spark.fanout.enabled. Default: false. 
  - check-ordering - check if input schema and table schema are same. Default: true
- The write mode can only be overwrite or append

Actor Class: `com.qwshen.etl.sink.IcebergWriter`

The definition of the IcebergWriter:

- In YAML format
```yaml
  actor:
    type: iceberg-writer
    properties:
      options:
        check-ordering: "true"
      mode: overwrite
      location: /tmp/users-warehouse
      view: users      
```
- In JSON format
```json
  {
    "actor": {
      "type": "iceberg-writer",
      "properties": {
        "options": {
          "check-orering": "true"
        },
        "mode": "overwrite",
        "location": "events.db.users",
        "view": "users"
      }
    }
  }
```
- In XML format
```xml
  <actor type="iceberg-writer">
    <properties>
      <options>
        <check-ordering>true</check-ordering>
      </options>
      <mode>overwrite</mode>
      <location>hdfs:///event-warehouse/users</location>
      <view>users</view>
    </properties>
  </actor>
```

Note:
1. When submitting a job for writing to an icerberg table, dynamic overwrite mode is recommended by setting spark.sql.sources.partitionOverwriteMode=dynamic.
2. The IcebergWriter provides only fundamental write operations. It is recommended to use SQL commands with SqlWriter for more complicated writes.
   1. INSERT INTO (for Spark 3.0 or later)
    ```
    INSERT INTO prod.db.table VALUES (1, 'a'), (2, 'b')
    -- or
    INSERT INTO prod.db.table SELECT ...
    ```
   2. MERGE INTO (for Spark 3.0 or later)
    ```
    MERGE INTO prod.db.target t
    USING (SELECT * from source) s
    ON t.id = s.id
    WHEN MATCHED AND s.op = 'delete' THEN DELETE
    WHEN MATCHED AND t.count IS NULL AND s.op = 'increment' THEN UPDATE SET t.count = 0
    WHEN MATCHED AND s.op = 'increment' THEN UPDATE SET t.count = t.count + 1
    WHEN NOT MATCHED AND s.event_time > still_valid_threshold THEN INSERT (id, count) VALUES (s.id, 1)
    WHEN NOT MATCHED THEN INSERT *
    ```
   3. INSERT OVERWRITE (make sure Spark is 3.0.1 or later)
    ```
    INSERT OVERWRITE prod.my_app.logs
    SELECT uuid, first(level), first(ts), first(message)
    FROM prod.my_app.logs
    WHERE cast(ts as date) = '2020-07-01'
    GROUP BY uuid

    -- overwrite by partition
    INSERT OVERWRITE prod.my_app.logs
    PARTITION (level = 'INFO')
    SELECT uuid, first(level), first(ts), first(message)
    FROM prod.my_app.logs
    WHERE level = 'INFO'
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
