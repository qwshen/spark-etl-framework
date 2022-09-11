The FlightWriter is for writing Spark dataframes to an arrow-flight end-points by using [spark-flight-connector](https://github.com/qwshen/spark-flight-connector).

The following properties are mandatory:
- `host` - the full-name or ip of an arrow-flight end-point host.
- `port` - the port number for connecting to the host.
- `user` & `password` - the user-name and password for the connection
- `table` - the name of a table to which the connector writes data.
- `mode` - the save-mode. It must be either `overwrite` or `append`.

The following properties are optional:
- `tls.enabled`: whether the arrow-flight end-point is tls-enabled for secure communication;
- `tls.verifyServer` - whether to verify the certificate from the arrow-flight end-point; Default: true if `tls.enabled = true`.
- `tls.truststore.jksFile`: the trust-store file in jks format;
- `tls.truststore.pass`: the pass code of the trust-store;
- `column.quote`: the character to quote the name of fields if any special character is used, such as the following sql statement:
  ```roomsql
  update flights set "departure-time" = '2022-01-03 11:26', "arrival-time" = '2022-01-03 16:33' where "flight-no" = 'ABC-21';
  ```
- `write.protocol`: the protocol of how to submit DML requests to flight end-points. It must be one of the following:
  - `prepared-sql`: the connector uses PreparedStatement of Flight-SQL to conduct all DML operations.
  - `literal-sql`: the connector creates literal sql-statements for all DML operations. Type mappings between arrow and target flight end-point may be required, please check the Type-Mapping section below. This is the default protocol.
- `batch.size`: the number of rows in each batch for writing. The default value is 1024. Note: depending on the size of each record, StackOverflowError might be thrown if the batch size is too big. In such case, adjust it to a smaller value.
- `merge.byColumn`: the name of a column used for merging the data into the target table. This only applies when the save-mode is `append`;
- `merge.ByColumns`: the name of multiple columns used for merging the data into the target table. This only applies when the save-mode is `append`.

For more details about all properties, please check [spark-flight-connector](https://github.com/qwshen/spark-flight-connector).

Actor Class: `com.qwshen.etl.sink.FlightWriter`

The definition of the FlightWriter:

- In YAML format
```yaml
  actor:
    type: flight-writer
    properties:
      connection:
        host: 192.168.0.26
        port: 32101
        user: test
        password: password123
        table: \"e-commerce\".orders
      options:
        tls.enabled: true
        tls.verifyServer: true
        column.quote: \"
      mode: overwrite
```

- In JSON format
```json
  {
    "actor": {
      "type": "flight-writer",
      "properties": {
        "connection": {
          "host": "192.168.0.26",
          "port": 32101,
          "user": "test",
          "password": "password123",
          "table": "\"e-commerce\".orders"
        },
        "options": {
          "tls.enabled": true,
          "tls.verifyServer": true,
          "column.quote": "\"",
          "batch.size": 4096,
          "merge.byColumn": "order_id"
        },
        "mode": "append"
      }
    }
  }
```

- In XML format
```xml
  <actor type="flight-writer">
    <properties>
      <connection>
        <host>192.168.0.26</host>
        <port>32101</port>
        <user>test</user>
        <password>password123</password>
        <table>"e-commerce".orders</table>
      </connection>
      <options>
        <tls.enabled>true</tls.enabled>
        <tls.verifyServer>true</tls.verifyServer>
        <column.quote>"</column.quote>
      </options>
      <mode>overwrite</mode>
    </properties>
  </actor>
```

Import Note: when this writer is used in a pipeline, the [spark-flight-connector](https://github.com/qwshen/spark-flight-connector) is required when submitting a job. Please download the jar and make it available for the job.