The SqlReader is for executing Spark-Sql select-statements to load data from source tables. It may reference one or more tables with join relationship and produces one output view. Please note that the SqlReader can only run select-statements.

Actor Class: `com.qwshen.etl.source.SqlReader`

The definition for the SqlReader:

- In YAML format
```yaml
    actor:
      type: sql-reader
      properties:
        sqlString: >
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
      type: sql-reader
      properties:
        sqlFile: scripts/event_raw.sql
```

- In JSON format
```json
  {
    "actor": {
      "type": "sql-reader",
      "properties": {
        "sqlString": "select * from events_raw"
      }
    }
  }
```
or
```json
  {
    "actor": {
      "type": "sql-reader",
      "properties": {
        "sqlFile": "scripts/event_raw.sql"
      }
    }
  }
```

- In XML format
```xml
    <actor type="sql-reader">
        <properties>
            <sqlString>
                select
                    substr(row_value, 1, 12) as event_id,
                    substr(row_value, 13, 16) as event_time,
                    substr(row_value, 29, 12) as event_host,
                    substr(row_value, 41, 64) as event_location
                from events_raw
                where row_no not in (1, 2)
            </sqlString>
        </properties>
    </actor>
```
or
```xml
    <actor type="sql-reader">
        <properties>
            <sqlFile>scripts/event_raw.sql</sqlFile>
        </properties>
    </actor>
```
