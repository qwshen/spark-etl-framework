The DeltaReader is for reading from delta lake in batch mode.

- The supported read-options are as follows
    - timestampAsOf - to specify the timestamp of data to be read from data lake
    - versionAsOf - to specify the version of data to be read from data lake
- The location of the reading can be either by sourcePath or sourceTable. If both specified, sourceTable takes precedence.

Actor Class: `com.qwshen.etl.source.DeltaReader`

The definition of the DeltaReader:

- In YAML format
```yaml
  actor:
    type: delta-reader
    properties:
      options:
        timestampAsOf: "2018-09-18T11:15:21.021Z"
        versionAsOf: "11"
      sourcePath: /tmp/users
```
- In JSON format
```json
  {
    "actor": {
      "type": "delta-reader",
      "properties": {
        "options": {
          "timestampAsOf": "2018-09-18T11:15:21.021Z",
          "versionAsOf": "11"
        },
        "sourceTable": "users"
      }
    }
  }
```
- In XML format
```xml
  <actor type="delta-reader">
    <properties>
      <options>
        <timestampAsOf>2018-09-18T11:15:21.021Z</timestampAsOf>
        <versionAsOf>11</versionAsOf>
      </options>
      <sourceTable>users</sourceTable>
    </properties>
  </actor>
```

Note:
- The DeltaReader provides only fundamental read operations. It is recommended to use SQL commands with SqlReader for more complicated reads.
    ```
    SELECT 
      uuid, first(level), first(ts), first(message)
    FROM default.logs
    WHERE cast(ts as date) = '2020-07-01'
    GROUP BY uuid;
  
    SELECT * FROM delta.`/tmp/delta/people10m`;
    ```
- To retrieve the history of a delta table including version, and timestamp, etc.:
```
DESCRIBE HISTORY delta.`/tmp/delta/people10m`;

DESCRIBE DETAIL delta.`/tmp/delta/people10m`;
```

Please use SqlActor to create/alter/drop iceberg tables, including calling stored-procedures:
```
CREATE or REPLACE TABLE default.logs (
    id bigint,
    data string,
    category string,
    ts timestamp)
USING delta
PARTITIONNED BY (category)
LOCATION '/events/logs'; -- unmanaged table

CREATE or REPLACE delta.`/events/logs` (
    id bigint,
    data string,
    category string,
    ts timestamp)
USING delta
PARTITIONNED BY (category);
```

```
ALTER TABLE table_name ADD COLUMNS (col_name data_type [COMMENT col_comment] [FIRST|AFTER colA_name], ...)

ALTER TABLE table_name ADD COLUMNS (col_name.nested_col_name data_type [COMMENT col_comment] [FIRST|AFTER colA_name], ...)

ALTER TABLE table_name ALTER [COLUMN] col_name col_name data_type [COMMENT col_comment] [FIRST|AFTER colA_name]

ALTER TABLE table_name ALTER [COLUMN] col_name.nested_col_name nested_col_name data_type [COMMENT col_comment] [FIRST|AFTER colA_name]

ALTER TABLE table_name REPLACE COLUMNS (col_name1 col_type1 [COMMENT col_comment1], ...)

ALTER TABLE <table_name> RENAME COLUMN old_col_name TO new_col_name

ALTER TABLE default.people10m SET TBLPROPERTIES ('department' = 'accounting', 'delta.appendOnly' = 'true');

-- show the table's properties.
SHOW TBLPROPERTIES default.people10m;

-- show just the 'department' table property.
SHOW TBLPROPERTIES default.people10m ('department');
```

```
VACUUM eventsTable   -- vacuum data files not required by versions older than the default retention period. 
  -- Does not delete Delta log files; log files are automatically cleaned up after checkpoints are written.
    
VACUUM '/data/events' -- vacuum files in path-based table

VACUUM delta.`/data/events/`

VACUUM delta.`/data/events/` RETAIN 100 HOURS  -- vacuum files not required by versions more than 100 hours old

VACUUM eventsTable DRY RUN    -- do dry run to get the list of files to be deleted
```