The SqlTransformer is a renamed of SqlActor which is for executing Spark-Sql statements. It may reference one or more views and produces one output view. Actually SqlTransform can act on any sql-statements, normally it is for running data transformation in the middle of a process by sql-statements.

Actor Class: `com.qwshen.etl.transform.SqlTransformer`

The definition for the SqlTransformer:

- In YAML format
```yaml
    actor:
      type: sql
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
      type: sql
      properties:
        sqlFile: scripts/event_raw.sql
```

- In JSON format
```json
  {
    "actor": {
      "type": "sql",
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
      "type": "sql",
      "properties": {
        "sqlFile": "scripts/event_raw.sql"
      }
    }
  }
```

- In XML format
```xml
    <actor type="sql">
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
    <actor type="sql">
        <properties>
            <sqlFile>scripts/event_raw.sql</sqlFile>
        </properties>
    </actor>
```
