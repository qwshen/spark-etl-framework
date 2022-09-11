The FlightReader is for reading datasets from arrow-flight end-points by using [spark-flight-connector](https://github.com/qwshen/spark-flight-connector).

The following properties are mandatory:
- `host` - the full-name or ip of an arrow-flight end-point host.
- `port` - the port number for connecting to the host.
- `user` & `password` - the user-name and password for the connection
- `table` - the name of a table from which the connector reads data. The table can be a physical data table, or any view. It can also be a select sql-statement or tables-joining statement. For example:
  - Select statement:
    ```roomsql
    select id, name, address from customers where city = 'TORONTO'
    ```
  - Join tables statement:
    ```roomsql
    orders o inner join customers c on o.customer_id = c.id
    ```

The following properties are optional:
- `tls.enabled`: whether the arrow-flight end-point is tls-enabled for secure communication;
- `tls.verifyServer` - whether to verify the certificate from the arrow-flight end-point; Default: true if `tls.enabled = true`.
- `tls.truststore.jksFile`: the trust-store file in jks format;
- `tls.truststore.pass`: the pass code of the trust-store;
- `column.quote`: the character to quote the name of fields if any special character is used, such as the following sql statement:
  ```roomsql
  select id, "departure-time", "arrival-time" from flights where "flight-no" = 'ABC-21';
  ```
- `partition.size`: the number of partitions in the final dataframe. The default is 6.
- `partition.byColumn`: the name of a column used for partitioning. Only one column is supported. This is mandatory when custom partitioning is applied.
- `partition.lowerBound`: the lower-bound of the by-column. This only applies when the data type of the by-column is numeric or date-time.
- `partition.upperBound`: the upper-bound of the by-column. This only applies when the data type of the by-column is numeric or date-time.
- `partition.hashFunc`: the name of the hash function supported in the arrow-flight end-points. This is required when the data-type of the by-column is not numeric or date-time, and the lower-bound, upper-bound are not provided. The default name is the hash as defined in Dremio.
- `partition.predicate`: each individual partitioning predicate is prefixed with this key.
- `partition.predicates`: all partitioning predicates, concatenated by semi-colons (;) or commas (,).

For more details about all properties, please check [spark-flight-connector](https://github.com/qwshen/spark-flight-connector).

Actor Class: `com.qwshen.etl.source.FlightReader`

The definition of the FlightReader:

- In YAML format
```yaml
  actor:
    type: flight-reader
    properties:
      connection:
        host: "192.168.0.26"
        port: 32101
        user: test
        password: password123
        table: "\"e-commerce\".orders"
      options:
        tls.enabled: true
        tls.verifyServer: true
        column.quote: \"
```

- In JSON format
```json
  {
    "actor": {
      "type": "flight-reader",
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
          "column.quote": "\""
        }
      }
    }
  }
```

- In XML format
```xml
  <actor type="flight-reader">
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
    </properties>
  </actor>
```

Import Note: when this reader is used in a pipeline, the [spark-flight-connector](https://github.com/qwshen/spark-flight-connector) is required when submitting a job. Please download the jar and make it available for the job.
