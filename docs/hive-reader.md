The HiveReader is for reading data from a Hive environment. Either a sql-statement (via sqlString or sqlFile) or a hive-table must be defined.

When reading from a hive-table, a filter in sql syntax may be specified to limit what data is to be read.

The definition for the HiveReader:

- In YAML format
```yaml
    actor:
      type: hive-reader
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
      type: hive-reader
      properties:
        table: eCommerce.users
        filter: first_name like 'J%'
```

- In JSON format
```json
  {
    "actor": {
      "type": "hive-reader",
      "properties": {
        "table": "ref.periods"
      }
    }
  }
```
or
```json
  {
    "actor": {
      "type": "hive-reader",
      "properties": {
        "sqlFile": "scripts/event_raw.sql"
      }
    }
  }
```

- In XML format
```xml
    <actor type="hive-reader">
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
    <actor type="hive-reader">
        <properties>
            <sqlFile>scripts/event_raw.sql</sqlFile>
        </properties>
    </actor>
```