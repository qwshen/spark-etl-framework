The MongoWriter is for writing a data-frame to MongoDB in stream mode.

- The following connection properties must be provided in order to connect to target MongoDB
    - host: the host name of the target mongod instance.
    - port: the port number of the target mongod instance.
    - database: the database name to read data from.
    - collection: the collection name to read data from.
    - user: the user name for accessing the target mongod instance.
    - password: the password for authentication
- The following options control the reading behavior:
    - extendedBsonTypes: enables the extended BSON types when writing to MongoDB. Default: true.
    - localThreshold: the time in milliseconds to choose among multiple MongoDB servers to send a request. Default: 15.
    - replaceDocument: replace the whole document when saving Datasets that contain an _id field. If false it will only update the fields in the document that match the fields in the Dataset. Default: true.
    - maxBatchSize: the maximum batch size for bulk operations when saving data. Default: 512.
    - writeConcern.w: the write concern w option requests acknowledgment that the write operation has propagated to a specified number of mongod instances or to mongod instances with specified tags:
        - majority: requests acknowledgment that write operations have propagated to the calculated majority of the data-bearing voting members.
        - ```<number>```: requests acknowledgment that the write operation has propagated to the specified number of mongod instances. Default: 1
        - ```<custom write concern name>```: requests acknowledgment that the write operations have propagated to tagged members that satisfy the custom write concern defined in settings.getLastErrorModes. see https://docs.mongodb.com/manual/tutorial/configure-replica-set-tag-sets/#std-label-configure-custom-write-concern.
    - writeConcern.journal: the write concern j option requests acknowledgment from MongoDB that the write operation has been written to the on-disk journal.
        - If j: true, requests acknowledgment that the mongod instances, as specified in the w: <value>, have written to the on-disk journal.
    - writeConcern.wTimeoutMS: the write concern wTimeout option specifies a time limit, in milliseconds, for the write concern. The wtimeout is only applicable for w values greater than 1.
    - shardKey: the field by which to split the collection data. The field should be indexed and contain unique values. Default: _id.
    - forceInsert: forces saves to use inserts, even if a Dataset contains _id. Default: false.
    - ordered: sets the bulk operations ordered property. Default: true.
- The checkpointLocation must be specified as one write-option.
- The trigger mode must be one of the following values:
  - continuous - trigger a continuous query to checkpoint by an interval
  - processingTime - trigger a micro-batch query to start (one micro-batch) by an interval
  - once - trigger the streaming process one time
- The output mode must be one of the following values:
  - complete - all the rows in the streaming DataFrame/Dataset will be written to the sink every time there are some updates.
  - append - only the new rows in the streaming DataFrame/Dataset will be written to the sink.
  - update - only the rows that were updated in the streaming DataFrame/Dataset will be written to the sink every time there are some updates.
- The test.waittimeMS is for testing purpose which specify how long the streaming run will be last.

For more details of writer options, please check https://docs.mongodb.com/spark-connector/current/configuration/#std-label-spark-output-conf.

<b>Important Note:</b> When submitting a job to write to MongoDB, please provide the following configuration either through command argument or runtime-config in the application configuration file:    
```spark.mongodb.output.uri=mongodb://mongod-server:port/database.collection```

Example:

- Submitting a job
  ```
  spark-submit --master local --conf "spark.mongodb.output.uri=mongodb://localhost:27017/events.users" ...
  ```
- In application configuration
  ```
  application.runtime {
    spark {
      mongodb.output.uri = "mongodb://localhost:27017/events.users"
      ...
    }
  ```

Actor Class: `com.qwshen.etl.sink.MongoStreamWriter`

The definition of the MongoWriter:
- In YAML format
```yaml
  actor:
    type: mongo-stream-writer
    properties:
      host: localhost
      port: 27017
      database: events
      collection: orders
      user: power_user
      password: password
      options:
        replaceDocument: false
        maxBatchSize: 1024
        writeConcern.w: majority
        shardKey: order_id
        checkpointLocation: /tmp/redis/staging/users
      trigger:
        mode: once
        interval: 2 minutes
      outputMode: complete
      test:
        waittimeMS: 9000
      view: users
```
- In JSON format
```json
  {
    "actor": {
      "type": "mongo-stream-writer",
      "properties": {
        "host": "localhost",
        "port": "27017",
        "database": "events",
        "collection": "orders",
        "user": "power_user",
        "password": "password",
        "options": {
          "replaceDocument": "false",
          "writeConcern.w": "majority",
          "shardKey": "order_id",
          "maxBatchSize": "16000",
          "checkpointLocation": "/tmp/redis/staging/users"
        },
        "trigger": {
          "mode": "continuous",
          "interval": "3 minutes"
        },
        "outputMode": "append",
        "test": {
          "waittimeMS": "16000"
        },
        "view": "users"
      }
    }
  }
```
- In XML format
```xml
  <actor type="mongo-writer">
    <properties>
      <host>localhost</host>
      <port>27017</port>
      <database>events</database>
      <collection>orders</collection>
      <user>power_user</user>
      <password>password</password>
      <options>
        <replaceDocument>false</replaceDocument>
        <writeConcern.w>majority</writeConcern.w>
        <shardKey>order_id</shardKey>
        <maxBatchSize>16000</maxBatchSize>
        <checkpointLocation>/tmp/redis/staging/users</checkpointLocation>
      </options>
      <trigger>
        <mode>processingTime</mode>
        <interval>5 minutes</interval>
      </trigger>
      <outputMode>update</outputMode>
      <test>
        <waittimeMS>15000</waittimeMS>
      </test>
      <view>users</view>
    </properties>
  </actor>
```
