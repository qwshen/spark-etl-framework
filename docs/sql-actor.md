The SqlActor is a generic sql actor which can be used to execute any sql-statements, including select, insert, update, merge, delete, create, alter, etc.

The sql statement can be specified by either sqlString or sqlFile property. However, one of them must be defined.

Actor Class: `com.qwshen.etl.common.SqlActor`

The definition for the SqlActor:

- In YAML format
```yaml
    actor:
      type: sql-actor
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
      type: sql-actor
      properties:
        sqlFile: scripts/event_raw.sql
```

- In JSON format
```json
  {
    "actor": {
      "type": "sql-actor",
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
      "type": "sql-actor",
      "properties": {
        "sqlFile": "scripts/event_raw.sql"
      }
    }
  }
```

- In XML format
```xml
    <actor type="sql-actor">
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
    <actor type="sql-actor">
        <properties>
            <sqlFile>scripts/event_raw.sql</sqlFile>
        </properties>
    </actor>
```

The sql-statement specified by sqlString or from sqlFile can have multiple valid sql-sub-statements separated by semi-colon (;), including set statements. For example:
```roomsql
  set run_date = concat('${runActor}', ' at ', '${runTime}');
  with t as (
    select distinct
      u.user_id,
      u.gender,
      cast(u.birthyear as int) as birthyear,
      t.timestamp,
      t.interested,
      concat('${application.process_date}', '-', '${run_date}') as process_date,
      t.event as event_id
    from train t
      left join users u on t.user = u.user_id
  )
  select * from t      
```

In the above example:
- The ```${runActor}``` and ```${runTime}``` are defined in either application.conf, job-submit arguments or pipeline;
- The ```${run_date}``` is referenced in the next sql-statement;
- The ```${run_date}``` is also available in any downstream actors. 

**However, with such "complex" sql-statement, only the result of the last sql-sub-statement is outputted.**
