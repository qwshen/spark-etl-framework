The RedisWriter is for reading data from Redis into a dataframe in batch mode.

- The following connection properties must be provided in order to connect to target Redis
    - host: the host name where Redis is running
    - port: the port number for connecting to Redis. The default port is 6379.
    - dbNum: the number of the database in Redis
    - dbTable: the table name
    - authPassword: the password for authentication
- The following options controls the writing behavior:
    - key.column: the name of the key column. If multiple columns forms the key, combine them before writing to Redis
    - key.pattern: match the keys based on the pattern.
    - infer.schema: automatically infer the schema based on a random row
    - partitions.number: the number of partitions
    - max.pipeline.size: maximum number of commands per pipeline (used to batch commands)
    - scan.count: count option of SCAN command (used to iterate over keys)
    - iterator.grouping.size: the number of items to be grouped when iterating over underlying RDD partition
    - timeout: timeout in milli-seconds for connection
- The schema of the output dataframe can be defined in DDL format by key ddlSchemaString or ddlSchemaFile. This is optional.

The definition of the RedisReader:
- In YAML format
```yaml
  actor:
    type: redis-reader
    properties:
      host: localhost
      port: 6379
      dbNum: 11
      dbTable: users
      authPassword: password
      options:
        key.column: user_id
        key.pattern: 1112_*
        iterator.grouping.size: 1600
        scan.count: 240
        max.pipeline.size: 160
        timeout: 1600
      ddlSchemaString: "user_id string, gender string, birth_year int, joined_at string"
```
- In JSON format
```json
  {
    "actor": {
      "type": "redis-reader",
      "properties": {
        "host": "localhost",
        "port": "6379",
        "dbNum": "2",
        "dbTable": "users",
        "authPassword": "password",
        "options": {
          "key.column": "user_id",
          "key.pattern": "1112_*",
          "iterator.grouping.size": "1600",
          "scan.count": "240",
          "max.pipeline.size": "160",
          "timeout": "1600"
        },
        "ddlSchemaFile": "./schemas/users.ddl",
      }
    }
  }
```
- In XML format
```xml
  <actor type="redis-reader">
    <properties>
      <host>localhost</host>
      <port>6379</port>
      <dbNum>2</dbNum>
      <dbTable>users</dbTable>
      <authPassword>password</authPassword>
      <options>
        <key.column>user_id</key.column>
        <iterator.grouping.size>1600</iterator.grouping.size>
        <scan.count>240</scan.count>
        <max.pipeline.size>160</max.pipeline.size>
        <timeout>1600</timeout>
      </options>
    </properties>
  </actor>
```