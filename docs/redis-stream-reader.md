The RedisStreamWriter is for reading data from Redis into a dataframe in streaming mode.

- The following connection properties must be provided in order to connect to target Redis
    - host: the host name where Redis is running
    - port: the port number for connecting to Redis. The default port is 6379.
    - dbNum: the number of the database in Redis
    - dbTable: the table name
    - authPassword: the password for authentication
- The following options controls the writing behavior:
    - stream.keys: the name of the key columns for partitioning the data. If multiple columns are required, separate them by comma.
    - stream.offsets: defines the start point of the stream in JSON string. The following is one example:
      ```
      val offsets = """{
        "offsets": {
          "sensors": {
            "groupName": "redis-source",
            "offset":"1548083485360-0"
          }
        }
      }"""
      ...
      .option("stream.offsets", offsets)
      ```
      This sets the offset id to be 1548083485360-0 for the consumer group name - redis-source. If the stream starts from the beginning, set offset id to 0-0.
    - stream.parallelism: the number of consumers for each single key. Each consumer is mapped to a partition.
    - stream.group.name: the name of the consumer group. By default, the name is spark-source.
    - stream.consumer.prefix: the prefix of the consumer name in the consumer group.
    - stream.read.batch.size: the maximum number of pulled items
    - stream.read.block: the time in milliseconds to wait in a XREADGROUP call
    - checkpointLocation: the location for checkpointing.
    - infer.schema: automatically infer the schema based on a random row
    - partitions.number: the number of partitions
    - max.pipeline.size: maximum number of commands per pipeline (used to batch commands)
    - scan.count: count option of SCAN command (used to iterate over keys)
    - iterator.grouping.size: the number of items to be grouped when iterating over underlying RDD partition
    - timeout: timeout in milli-seconds for connection  
      _**Note: for details, please check https://github.com/RedisLabs/spark-redis**_
- The schema of the output dataframe can be defined in DDL format by key ddlSchemaString or ddlSchemaFile. This is optional.
- For watermark configuration, the timeField is one field in the dataframe to be used for the delay calculation.
- To add a custom (processing) timestamp, please use the addTimestamp property. This column is added as the name of __timestamp.

Actor Class: `com.qwshen.etl.source.RedisStreamReader`

The definition of the RedisReader:
- In YAML format
```yaml
  actor:
    type: redis-stream-reader
    properties:
      host: localhost
      port: 6379
      dbNum: 11
      dbTable: users
      authPassword: password
      options:
        stream.keys: user_id
        checkpointLocation: "/tmp/redis/checkpoing/users"
      ddlSchemaString: "user_id string, gender string, birth_year int, joined_at string"
      watermark:
        timeField: __timestamp
        delayThreshold: 5 minutes
      addTimestamp: true
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
          "stream.keys": "user_id",
          "checkpointLocation": "/tmp/redis/checkpoint/users"
        },
        "ddlSchemaFile": "./schemas/users.ddl",
        "watermark": {
          "timeField": "__timestamp",
          "delayThreshold": "5 minutes"
        },
        "addTimestamp": "true"
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
        <stream.keys>user_id</stream.keys>
        <checkpointLocation>/tmp/redis/checkpoint/users</checkpointLocation>
      </options>
      <watermark>
        <timeField>__timestamp</timeField>
        <delayThreshold>5 minutes</delayThreshold>
      </watermark>
      <addTiemstamp>true</addTiemstamp>
    </properties>
  </actor>
```
