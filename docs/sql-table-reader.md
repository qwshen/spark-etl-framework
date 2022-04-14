The SqlTableReader is for reading data from a source table. A filter in sql syntax may be specified to limit what data is to be read.

Actor Class: `com.qwshen.etl.source.SqlTableReader`

The definition for the SqlTableReader:

- In YAML format
```yaml
    actor:
      type: sql-table-reader
      properties:
        table: eCommerce.users
        filter: first_name like 'J%'
```

- In JSON format
```json
  {
    "actor": {
      "type": "sql-table-reader",
      "properties": {
        "table": "ref.periods"
      }
    }
  }
```

- In XML format
```xml
    <actor type="hive-reader">
        <properties>
            <table>events.db.events</table>
            <filter>first_name like 'J%'</filter>
        </properties>
    </actor>
```
