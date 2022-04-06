The HiveWriter is for writing a Spark Dataframe into a Hive table. The writing behavior can be defined by using an insert-into or insert-overwrite statement, or giving a hive-table to be written into.

The sql-statement can be either sqlString or sqlFile.

When a hive-table is given, the following properties can be defined to further control the writing behavior:
- table: the target table name in hive environment.
- partitionBy: columns separated by comma used to partition the data before writing
- numPartitions: the number of partitions for the dataframe to be partitioned into before writing.
- mode: it must be either overwrite or append. When overwrite, the whole target table will be over-written.
- view: the input view (dataframe) to be written

Actor Class: `com.qwshen.etl.sink.HiveWriter`

The definition for the HiveWriter:

- In YAML format
```yaml
    actor:
      type: hive-writer
      properties:
        sqlString: >
          insert overwrite table eCommerce.events
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
      type: hive-writer
      properties:
        table: eCommerce.events
        mode: overwrite
        view: events
```

- In JSON format
```json
  {
    "actor": {
      "type": "sql",
        "properties": {
          "table": "eCommerce.users",
          "partitionBy": "age, gender",
          "numPartitions": "16",
          "mode": "append",
          "view": "users"
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
                insert into table eCommerce.users 
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
            <table>eCommerce.users</table>
            <partitionBy>gender, age</partitionBy>
            <numPartitions>16</numPartitions>
            <mode>append</mode>
            <view>users</view>
        </properties>
    </actor>
```
