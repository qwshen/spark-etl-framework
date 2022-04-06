The RedisWriter is for writing dataframe to Redis environment in batch mode.

- The following connection properties must be provided in order to connect to target Redis
  - host: the host name where Redis is running
  - port: the port number for connecting to Redis. The default port is 6379.
  - dbNum: the number of the database in Redis
  - dbTable: the table name 
  - authPassword: the password for authentication
- The following options controls the writing behavior:
  - model: hash or binary. Default is to hash.   
    - By default DataFrames are persisted as Redis Hashes. It allows for data to be written with Spark and queried from a non-Spark environment. It also enables projection query optimization when only a small subset of columns are selected. On the other hand, there is currently a limitation with the Hash model - it doesn't support nested DataFrame schemas. One option to overcome this is to make your DataFrame schema flat. If it is not possible due to some constraints, you may consider using the Binary persistence model.  
    - With the Binary persistence model the DataFrame row is serialized into a byte array and stored as a string in Redis (the default Java Serialization is used). This implies that storage model is private to Spark-Redis library and data cannot be easily queried from non-Spark environments. Another drawback of the Binary model is a larger memory footprint. 
  - filter.keys.by.type: true or false. Default is false. It's to make sure the underlying data structures match persistence model.
  - key.column: the name of the key column. If multiple columns forms the key, combine them before writing to Redis
  - ttl: Time to live in seconds. Redis expires data after the ttl
  - iterator.grouping.size: the number of items to be grouped when iterating over underlying RDD partition
  - scan.count: count option of SCAN command (used to iterate over keys)
  - max.pipeline.size: maximum number of commands per pipeline (used to batch commands)
  - timeout: timeout in milli-seconds for connection
- The mode must be either overwrite or append.

Actor Class: `com.qwshen.etl.sink.RedisWriter`

The definition of the RedisWriter:
- In YAML format
```yaml
  actor:
    type: redis-writer
    properties:
      host: localhost
      port: 6379
      dbNum: 11
      dbTable: users
      authPassword: password
      options:
        model: binary
        filter.keys.by.type: true
        key.column: user_id
        ttl: 72000
        iterator.grouping.size: 1600
        scan.count: 240
        max.pipeline.size: 160
        timeout: 1600
      mode: overwrite
      view: users
```
- In JSON format
```json
  {
    "actor": {
      "type": "redis-writer",
      "properties": {
        "host": "localhost",
        "port": "6379",
        "dbNum": "2",
        "dbTable": "users",
        "authPassword": "password",
        "options": {
          "model": "binary",
          "filter.keys.by.type": "true",
          "key.column": "user_id",
          "ttl": "72000",
          "iterator.grouping.size": "1600",
          "scan.count": "240",
          "max.pipeline.size": "160",
          "timeout": "1600"
        },
        "mode": "append",
        "view": "users"
      }
    }
  }
```
- In XML format
```xml
  <actor type="redis-writer">
    <properties>
      <host>localhost</host>
      <port>6379</port>
      <dbNum>2</dbNum>
      <dbTable>users</dbTable>
      <authPassword>password</authPassword>
      <options>
        <model>binary</model>
        <filter.keys.by.type>true</filter.keys.by.type>
        <key.column>user_id</key.column>
        <ttl>72000</ttl>
        <iterator.grouping.size>1600</iterator.grouping.size>
        <scan.count>240</scan.count>
        <max.pipeline.size>160</max.pipeline.size>
        <timeout>1600</timeout>
      </options>
      <mode>overwrite</mode>
      <view>users</view>
    </properties>
  </actor>
```
