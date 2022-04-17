The IcebergReader is for reading data from iceberg tables into data-frames in batch mode with DataFrame API.

- The table of the reading is the full name of an icerberg table.
- The supported read-options are as follows
  - snapshot-id - snapshot id of the table snapshot to read. Default: (latest)
  - as-of-timestamp - a timestamp in milliseconds. Default: latest - the snapshot used will be the snapshot current at this time.
  - split-size - the table’s read.split.target-size and read.split.metadata-target-size. Default: as per table property.
  - lookback - the table’s read.split.planning-lookback. Default: as per table property.
  - file-open-cost - the table’s read.split.open-file-cost. Default: as per table property.
  - vectorization-enabled - the table’s read.parquet.vectorization.enabled. Default: as per table property.
  - batch-size - the table’s read.parquet.vectorization.batch-size. Default: as per table property.
- Options for incremental read:
  - start-snapshot-id - start snapshot ID used in incremental scans (exclusive)
  - end-snapshot-id - end snapshot ID used in incremental scans (inclusive). This is optional. Omitting it will default to the current snapshot

Please note:
- Time travel is not yet supported by Spark’s SQL syntax.
- Incremental read currently gets only the data from append operation. Cannot support replace, overwrite, delete operations.

Actor Class: `com.qwshen.etl.source.IcebergReader`

The definition of the IcebergReader:

- In YAML format
```yaml
  actor:
    type: iceberg-reader
    properties:
      options:
        batch-size: "6400"
      table: events.db.users
```
- In JSON format
```json
  {
    "actor": {
      "type": "iceberg-reader",
      "properties": {
        "table": "events.db.users",
        "options": {
          "snapshot-id": "2342438929304"
        }
      }
    }
  }
```
- In XML format
```xml
  <actor type="iceberg-reader">
    <properties>
      <options>
        <start-snapshot-id>23423424324</start-snapshot-id>
        <end-snapshot-id>23423483234</end-snapshot-id>  
      </options>
      <table>events.db.users</table>
    </properties>
  </actor>
```

Note:
- The IcebergReader provides only fundamental read operations. It is recommended to use SQL commands with SqlReader for more complicated reads. However, incremental read is not supported by Spark’s SQL syntax.
    ```
    SELECT 
      uuid, first(level), first(ts), first(message)
    FROM prod.my_app.logs
    WHERE cast(ts as date) = '2020-07-01'
    GROUP BY uuid
    ```

- To inspect the history of a table:
```
  -- sql
  select * from events.db.users.history
```
```
  //dataframe api
  spark.read.format("iceberg").load("file:///tmp/events/db/users#history").show
```

- To show the valid snapshots of a table:
```
  -- sql
  select * from events.db.users.snapshots
```
```
  //dataframe api
  spark.read.format("iceberg").load("file:///tmp/events/db/users#snapshots").show
```


- To show the data files of a table:
```
  -- sql
  select * from events.db.users.files
```
```
  //dataframe api
  spark.read.format("iceberg").load("file:///tmp/events/db/users#files").show
```

- To show the file manifests of a table:
```
  -- sql
  select * from events.db.users.manifests
```
```
  //dataframe api
  spark.read.format("iceberg").load("file:///tmp/events/db/users#manifests").show
```

Please use SqlActor to create/alter/drop iceberg tables, including calling stored-procedures:
```
CREATE/REPLACE TABLE prod.db.sample (
    id bigint,
    data string,
    category string,
    ts timestamp)
USING iceberg
PARTITIONED BY (bucket(16, id), days(ts), category)
```

```
ALTER TABLE prod.db.sample ADD COLUMN point.z double
```

```
ALTER TABLE prod.db.sample WRITE DISTRIBUTED BY PARTITION LOCALLY ORDERED BY category, id
```