The JdbcStreamWriter is for writing a Spark dataframe into a table in a relational database in streaming mode.

- The connection property may contain the following entries:
    - driver: the database base driver, such as com.mysql.jdbc.Driver.
    - url: the jdbc url for connecting to target database.
    - dbtable: the name of the table from which to read data.
    - user & password: the username and password for the connection.
- The following write options may be applied when writing to a relational database:
    - numPartitions: defines the number of partitions used to control the concurrency of writing.
    - batchSize: defines the number of rows for each batch when writing
    - isolationLevel: defines the isolation level of transactions
- The checkpointLocation can be specified as one write-option.
- The trigger mode must be one of the following values:
  - continuous
  - processingTime
  - once
- The output mode must be one of the following values:
  - complete
  - append
  - update
- The test.waittimeMS is for testing purpose which specify how long the streaming run will be last.
- The sink.SqlString or sink.SqlFile defines how the new data is merged into the target table. It normally is a merge into statement.

The definition of JdbcStreamWriter
- In YAML format
```yaml
  actor:
    type: jdbc-stream-writer
    properties:
      connection:
        driver: com.mysql.jdbc.Driver
        url: jdbc:mysql://localhost:3306/events
        dbtable: users
        user: root
        password: root-password
      options:
        numPartitions: 9
        batchSize: 1024
        isolationLevel: READ_UNCOMMITTED
        checkpointLocation: "/tmp/jdbc/writing"
      trigger:
        mode: continuous
        interval: 3 seconds
      outputMode: append
      test.waittimeMS: 30000
      sink:
        sqlString: >
          insert into products(id, name, description, price, batch_id) values(@id, @name, @description, @price, @batch_id)
            on duplicate key update
              name = @name,
              description = @description,
              price = @price
      view: users
```

- In JSON format
```json
    "actor": {
      "type": "jdbc-writer",
      "properties": {
        "connection": {
          "driver": "com.mysql.jdbc.Driver",
          "url": "jdbc:mysql://localhost:3306/events",
          "dbtable": "users",
          "user": "root",
          "password": "root-password"
        },
        "options": {
          "numPartitions": 9,
          "batchSize": "1024",
          "isolationLevel": "READ_COMMITTED"
        },
        "trigger": {
          "mode": "processingTime",
          "interval": "3 seconds"
        },
        "outputMode": "append",
        "test": {
          "waittimeMS": "3000"
        },
        "sink": {
          "sqlFile": "@{events.users.mergeStmt}"
        },
        "view": "users"
      }
    }
```

- In XML format
```xml
    <actor type="jdbc-writer">
      <properties>
        <connection>
          <driver>com.mysql.jdbc.Driver</driver>
          <url>jdbc:mysql://localhost:3306/events</url>
          <dbtable>users</dbtable>
          <user>root</user>
          <password>root-password</password>
        </connection>
        <options>
          <numPartitions>9</numPartitions>
          <batchSize>1024</batchSize>
          <truncate>true</truncate>
        </options>
        <trigger>
          <mode>continuous</mode>
          <interval>5 seconds</interval>
        </trigger>
        <outputMode>append</outputMode>
        <test>
          <waittimeMS>30000</waittimeMS>
        </test>
        <view>users</view>
      </properties>
    </actor>
```