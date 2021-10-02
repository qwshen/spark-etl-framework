The HBaseWriter is for writing a dataframe to a HBase table in batch mode.

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
- The following defines the data mapping of columns from source dataframe to the columns in the target HBase table:
  ```yaml
    columnsMapping:
      src-col1: "col-family1:dst-col1"     
      src-col2: "col-family2:dst-col2" 
      ...
  ```
  Where: src-col1, src-col2, ... are the columns from the source dataframe, and col-family1:dst-col1, col-family2:dst-col2, ... are the target columns in HBase.
- The row-key is defined as follows:
  ```yaml
    rowKey:
      concatenator: "^"
      from: src-col1,src-col2   # fields from source separated by comma
    _**Note: if the rowKey is not defined, then uuid will be generated.**_
  ```
  The above configuration creates the row-key of target HBase table as src-col1^src-col2  
  <br />  

- The options property defines the writing behavior.
- The mode defines how data wil be written to HBase. It must be either overwrite or merge.
  - overwrite - the target table will be cleaned out then write the new data
  - merge -> the new data will be merged into target table
  <br />

The definition of HBaseWriter:
- In YAML format
```yaml
  actor:
    type: hbase-writer
    properties:
      connection:
        hbase.zookeeper.quorum: 127.0.0.1
        hbase.zookeeper.property.clientPort: 2181
        zookeeper.znode.parent: "/hbase-unsecure"
        hadoop.security.authentication: kerberos
        hbase.security.authentication: kerberos
      table: "events_db:users"
      columnsMapping:
        user_id: "profile:user_id"
        birthyear: "profile:birth_year"
        gender: "profile:gender"
        address: "location:address"
      rowKey:
        concatenator: "^"
        from: user_id
      options:
        numPartitions: 16
        batchSize: 900
      mode: "overwrite"
      view: users
```

- In JSON format
```json
  {
    "actor": {
      "type": "hbase-writer",
      "properties": {
        "connection": {
          "core-site.xml": "/usr/hdp/current/hbase-server/conf/core-site.xml",
          "hdfs-site.xml": "/usr/hdp/current/hbase-server/conf/hdfs-site.xml",
          "hbase-site.xml": "/usr/hdp/current/hbase-server/conf/hbase-site.xml"
        },
        "table": "events_db:users",
        "columnsMapping": {
          "user_id":  "profile:user_id",
          "birthyear": "profile:birth_year",
          "gender": "profile:gender",
          "address": "location:address"
        },
        "rowKey": {
          "concatenator": "&",
          "from": "user_id,timestamp"
        },
        "options": {
          "numPartitions": "16",
          "batchSize": "9000"
        },
        "mode": "merge",
        "view": "users"
      }
    }
  }
```

- In XML format
```xml
  <actor type="hbase-writer">
    <properties>
      <connection>
          <core-site.xml>/usr/hdp/current/hbase-server/conf/core-site.xml</core-site.xml>
          <hdfs-site.xml>/usr/hdp/current/hbase-server/conf/hdfs-site.xml</hdfs-site.xml>
          <hbase-site.xml>/usr/hdp/current/hbase-server/conf/hbase-site.xml</hbase-site.xml>
      </connection>
      <table>events_db:users</table>
      <columnsMapping>
          <user_id>profile:user_id</user_id>
          <birthyear>profile:birth_year</birthyear>
          <gender>profile:gender</gender>
          <address>location:address</address>
      </columnsMapping>
      <rowKey>
          <concatenator>^</concatenator>
          <from>user_id,timestamp</from>
      </rowKey>
      <options>
          <numPartitions>16</numPartitions>
          <batchSize>9000</batchSize>
      </options>
      <mode>overwrite</mode>
      <view>users</view>
    </properties>
  </actor>
```
