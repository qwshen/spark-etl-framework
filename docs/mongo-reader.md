The MongoReader is for reading data from MongoDB into a dataframe in batch mode.

- The following connection properties must be provided in order to connect to target MongoDB
    - host: the host name of the target mongod instance.
    - port: the port number of the target mongod instance.
    - database: the database name to read data from.
    - collection: the collection name to read data from.
    - user: the user name for accessing the target mongod instance.
    - password: the password for authentication
- The following options control the reading behavior:
    - localThreshold: the time in milliseconds to choose among multiple MongoDB servers to send a request. Default: 15.
    - readPreference.name: The name of the Read Preference mode to use.
      - primary: all read operations use only the current replica set primary. This is the default read mode.
      - primaryPreferred: reading from primary preferred. If primary not available, read from secondary.
      - secondary: operations read only from the secondary members of the set.
      - secondaryPreferred: reading from secondary preferred. If secondary not available, read from primary.
    - readPreference.tagSets: the ReadPreference TagSets to use.
    - readConcern.level: the Read Concern level to use.
      - local: the query returns data from the instance with no guarantee that the data has been written to a majority of the replica set members (i.e. may be rolled back later).
      - available: the query returns data from an available instance with no guarantee that the data has been written to a majority of the replica set members (i.e. may be rolled back later).
      - majority: the query returns the data that has been acknowledged by a majority of the replica set members.
      - linearizable: the query returns data that reflects all successful majority-acknowledged writes that completed prior to the start of the read operation.
    - partitioner: the name of the partitioner to use to split collection data into partitions. Partitions are based on a range of values of a field (e.g. _ids 1 to 100).
      - MongoDefaultPartitioner: wraps the MongoSamplePartitioner. Default.
      - MongoSamplePartitioner: uses the average document size and random sampling of the collection to determine suitable partitions for the collection.
      - MongoShardedPartitioner.
      - MongoSplitVectorPartitioner.
      - MongoPaginateByCountPartitioner.
      - MongoPaginateBySizePartitioner.
    - partitionerOptions: the custom options to configure the partitioner.
      - MongoSamplePartitioner.
        - partitionKey: the field by which to split the collection data. Default: _id.
        - partitionSizeMB: the size (in MB) for each partition. Default: 64.
        - samplesPerPartition: the number of sample documents to take for each partition in order to establish a partitionKey range for each partition. Default: 10.
      - MongoShardedPartitioner.
        - shardKey: the field by which to split the collection data. Default: _id.
      - MongoSplitVectorPartitioner.
        - partitionKey: the field by which to split the collection data. Default: _id.
        - partitionSizeMB: the size (in MB) for each partition. Default: 64.
      - MongoPaginateByCountPartitioner.
        - partitionKey: the field by which to split the collection data. Default: _id.
        - numberOfPartitions: the number of partitions to create. Default: 64.
      - MongoPaginateBySizePartitioner.
        - partitionKey: the field by which to split the collection data. Default: _id.
        - partitionSizeMB: the size (in MB) for each partition. Default: 64.
    - allowDiskUse: enables writing to temporary files during aggregation.
    - batchSize: the size of the internal batches within the cursor.

The definition of the MongoReader:
- In YAML format
```yaml
  actor:
    type: mongo-reader
    properties:
      host: localhost
      port: 27017
      database: events
      collection: orders
      user: power_user
      password: password
      options:
        readPreference.name: secondaryPreferred
        readConcern.level: majority
        partitioner: MongoShardedPartitioner
        shardKey: order_id
        batchSize: 16000
```
- In JSON format
```json
  {
    "actor": {
      "type": "mongo-reader",
      "properties": {
        "host": "localhost",
        "port": "27017",
        "database": "events",
        "collection": "orders",
        "user": "power_user",
        "password": "password",
        "options": {
          "readPreference.name": "secondaryPreferred",
          "readConcern.level": "majority",
          "partitioner": "MongoShardedPartitioner",
          "shardKey": "order_id",
          "batchSize": "16000"
        }
      }
    }
  }
```
- In XML format
```xml
  <actor type="mongo-reader">
    <properties>
      <host>localhost</host>
      <port>27017</port>
      <database>events</database>
      <collection>orders</collection>
      <user>power_user</user>
      <password>password</password>
      <options>
        <readPreference.name>secondaryPreferred</readPreference.name>
        <readConcern.level>majority</readConcern.level>
        <partitioner>MongoShardedPartitioner</partitioner>
        <shardKey>order_id</shardKey>
        <batchSize>16000</batchSize>
      </options>
    </properties>
  </actor>
```