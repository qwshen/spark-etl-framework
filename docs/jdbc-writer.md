The JdbcWriter is for writing a Spark dataframe into a table in a relational database in batch mode.

- The connection property may contain the following entries:
    - driver: the database base driver, such as com.mysql.jdbc.Driver.
    - url: the jdbc url for connecting to target database.
    - dbtable: the name of the table from which to read data.
    - user & password: the username and password for the connection.

- The following read options may be applied when writing to a relational database:
    - numPartitions: defines the number of partitions used to control the concurrency of writing.
    - batchSize: defines the number of rows for each batch when writing
    - isolationLevel: defines the isolation level of transactions
- The mode defines how rows are written into database. It must be one of the following values:
    - overwrite: overwrite the target table with the new data. __When using this mode and also requiring to keep the existing schema of the table, please set truncate option to true. By default, the overwrite mode also overwrite the schema of the target table.__ 
    - append: append the new data into the target table
    - merge: insert or update the new data into the target table. When this option is used, the sink.SqlString or sink.SqlFile is required.
- The sink.SqlString or sink.SqlFile defines how the new data is merged into the target table. It normally is a merge into statement.

Actor Class: `com.qwshen.etl.sink.JdbcWriter`

The definition of JdbcWriter
- In YAML format
```yaml
  actor:
    type: jdbc-writer
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
      mode: merge
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
        "mode": "merge",
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
        <mode>overwrite</mode>
        <view>users</view>
      </properties>
    </actor>
```
