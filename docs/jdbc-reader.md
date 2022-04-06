The JdbcReader is for reading data from a table in a relational database in batch mode.

- The connection property may contain the following entries:
  - driver: the database base driver, such as com.mysql.jdbc.Driver.
  - url: the jdbc url for connecting to target database.
  - dbtable: the name of the table from which to read data.
  - user & password: the username and password for the connection.
  
- The following read options may be applied when reading from a relational database:
  - partitionColumn: partitions the table when reading in parallel from multiple workers, must be a numeric, date or timestamp
  - lowerBound: defines the start of the partition stride
  - upperBound: defines the end of the partition stride.  
    _Note: the above 3 options must be either all or none of them specified._ 
  - numPartitions: defines the maximum number of partitions that can be used for parallelism in table reading.
  - fetchSize: defines the # of records per fetch.

Actor Class: `com.qwshen.etl.source.JdbcReader`

The definition of JdbcReader
- In YAML format
```yaml
  actor:
    type: jdbc-reader
    properties:
      connection:
        driver: com.mysql.jdbc.Driver
        url: jdbc:mysql://localhost:3306/events
        dbtable: users
        user: root
        password: root-password
      options:
        numPartitions: 9
        fetchSize: 1024
```
 
- In JSON format
```json
    "actor": {
      "type": "jdbc-reader",
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
          "fetchSize": "1024"
        }
      }
    }
```

- In XML format
```xml
    <actor type="jdbc-reader">
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
          <fetchSize>1024</fetchSize>
        </options>
      </properties>
    </actor>
```
