The SqlWriter is a specialized SqlActor for executing insert/update/merge/delete-statements only to modify data in target tables. It may reference one or more tables with join relationship. 

Actor Class: `com.qwshen.etl.sink.SqlWriter`

The definition for the SqlWriter:

- In YAML format
```yaml
    actor:
      type: sql-writer
      properties:
        sqlString: >
          insert into events.db.events  
          select
            substr(row_value, 1, 12) as event_id,
            substr(row_value, 13, 16) as event_time,
            substr(row_value, 29, 12) as event_host,
            substr(row_value, 41, 64) as event_location
          from events_raw
          where row_no not in (1, 2)
```
or
```yaml
    actor:
      type: sql-writer
      properties:
        sqlFile: scripts/event_raw.sql
```

- In JSON format
```json
  {
    "actor": {
      "type": "sql-writer",
      "properties": {
        "sqlString": "insert overwrite events.db.events select * from events_raw"
      }
    }
  }
```
or
```json
  {
    "actor": {
      "type": "sql-writer",
      "properties": {
        "sqlFile": "scripts/event_raw.sql"
      }
    }
  }
```

- In XML format
```xml
    <actor type="sql-writer">
        <properties>
            <sqlString>
                merge into events.db.events e
                using (select * from events_raw) s
                on e.event_id = s.event_id
                when matched then update set e.event_time = s.event_time
                when not matched then insert *
            </sqlString>
        </properties>
    </actor>
```
or
```xml
    <actor type="sql-writer">
        <properties>
            <sqlFile>scripts/event_raw.sql</sqlFile>
        </properties>
    </actor>
```
