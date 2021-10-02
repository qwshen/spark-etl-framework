The HBaseReader is for reading a HBase table into a dataframe in batch mode.

- The following connection properties are required to be able to connect to a HBase environment
    1. connection.hbase.zookeeper.quorum: a comma-separated list of hosts on which ZooKeeper servers are running
    2. connection.hbase.zookeeper.property.clientPort: the port number at which ZooKeeper servers are running on each host
    3. connection.zookeeper.znode.parent: the root znode that will contain all the znodes created/used by HBase
    4. connection.core-site.xml: the core-site configuration
    5. connection.hdfs-site.xml: the hdfs-site configuration
    6. connection.hbase-site.xml: the hbase-site configuration  
       _**Note: if option 6 is configured, then option 1, 2, 3 are ignored.**_
- The following two options are mandatory in secured hbase environment
    7. connection.hadoop.security.authentication: it must be the value of kerberos
    8. connection.hbase.security.authentication: it must be the value of kerberos  
       <br />

- The table property specifies the target HBase table to which the data will be writing.
- The following defines the data mapping of columns from a source HBase table to columns of a dataframe:
  ```yaml
    columnsMapping:
      dst-col0: "__:rowKey"
      dst-col1: "col-family1:src-col1"
      dst-col2: "col-family2:src-col2"
      ...
  ```
  Where: 
  - dst-col0 in the target dataframe is from the row-key in the source HBase table
  - dst-col1, dst-col2 ... in the target dataframe are from the col-family1:src-col1, col-family2:src-col2, ... columns in the source HBase table.

- The options property defines the reading behavior.
  - maxResultSize: set the maximum result size. The default is -1; this means that no specific maximum result size will be set for this scan, and the global configured value will be used instead. (Defaults to unlimited).
  - batchSize: set the maximum number of values to return for each call.
  - isolationLevel: It must be either READ_COMMITTED or READ_UNCOMMITED.
  - numPartitions: the number of parititions for the output dataframe
- The filter defined what rows will be read from the source.
  - keyStart: the start value of the row key
  - keyStop: the end value of the row key
  - keyPrefix: the prefix of the row key
  - tsStart: the start timestamp of rows
  - tsEnd: the end timestamp of rows
       <br />

The definition of HBaseWriter:
- In YAML format
```yaml
  actor:
    type: hbase-reader
    properties:
      connection:
        hbase.zookeeper.quorum: 127.0.0.1
        hbase.zookeeper.property.clientPort: 2181
        zookeeper.znode.parent: "/hbase-unsecure"
        hadoop.security.authentication: kerberos
        hbase.security.authentication: kerberos
      table: "events_db:users"
      columnsMapping:
        user_id: "__:rowKey"
        birthyear: "profile:birth_year"
        gender: "profile:gender"
        address: "location:address"
      options:
        numPartitions: 16
        batchSize: 900
      filter: 
        tsStart: "2020-01-01 00:00:00"
        tsEnd: "2020-12-31 23:59:59"
```

- In JSON format
```json
  {
    "actor": {
      "type": "hbase-reader",
      "properties": {
        "connection": {
          "core-site.xml": "/usr/hdp/current/hbase-server/conf/core-site.xml",
          "hdfs-site.xml": "/usr/hdp/current/hbase-server/conf/hdfs-site.xml",
          "hbase-site.xml": "/usr/hdp/current/hbase-server/conf/hbase-site.xml"
        },
        "table": "events_db:users",
        "columnsMapping": {
          "user_id":  "__:rowKey",
          "birthyear": "profile:birth_year",
          "gender": "profile:gender",
          "address": "location:address"
        },
        "options": {
          "numPartitions": "16",
          "batchSize": "9000",
          "isolationLevel": "READ_COMMITTED"
        },
        "filter": {
          "tsStart": "2020-01-01 00:00:00",
          "tsEnd": "2020-12-31 23:59:59"
        }
      }
    }
  }
```

- In XML format
```xml
  <actor type="hbase-reader">
    <properties>
      <connection>
          <core-site.xml>/usr/hdp/current/hbase-server/conf/core-site.xml</core-site.xml>
          <hdfs-site.xml>/usr/hdp/current/hbase-server/conf/hdfs-site.xml</hdfs-site.xml>
          <hbase-site.xml>/usr/hdp/current/hbase-server/conf/hbase-site.xml</hbase-site.xml>
      </connection>
      <table>events_db:users</table>
      <columnsMapping>
          <user_id>__:rowKey</user_id>
          <birthyear>profile:birth_year</birthyear>
          <gender>profile:gender</gender>
          <address>location:address</address>
      </columnsMapping>
      <options>
          <numPartitions>16</numPartitions>
          <batchSize>9000</batchSize>
      </options>
      <filter>
        <tsStart>2020-01-01 00:00:00</tsStart>
      </filter>
    </properties>
  </actor>
```
