The KafkaStreamReader is for reading kafka topics in streaming mode.

* The startingOffsets/endingOffsets options are optional. They define the start/end point when reading from topics. It must be one of the following values:
  - latest (-1 in json) - read from the latest offsets.
  - earliest (-2 in json) - read from the very beginning.
  - {"topic-a": {"0":23, "1": -2}}, {"topic-b": {"0":-1, "1":-2, "3":100}}
  > **_Note:_** This only takes effect when there is no offsets-commit for the current reader.

* The kafka.group.id option defines the kafka group in which the KafkaReader is to read from kafka.
* The includeHeaders option is optional. It tells the KafkaReader whether retrieves the header of messages from kafka topics.
* The failOnDataLoss option is optional. It tells the KafkaReader whether fails the query when it's possible that data is lost (such as topics are deleted, or offsets are out of range). The default value is true.
* The kafkaConsumer.pollTimeoutMs option is optional. It defines the timeout in milliseconds to poll data from Kafka in executors. The default value is 512 milliseconds.
* The fetchOffset.numRetries option is optional. It defines the number of times to retry before giving up fetching kafka latest offsets. The default value is 3.
* The fetchOffset.retryIntervalMs option is optional. It defines the duration in milliseconds to wait before retrying to fetch kafka offsets. The default value is 10 milliseconds.
* The maxOffsetsPerTrigger option is optional. It defines the rate limit on maximum number of offsets processed per trigger interval. The specified total number of offsets will be proportionally split across topicPartitions of different volume. The default is none.
* The keySchema & valueSchema are optional. They can be defined by one of the following options:
  - avroSchemaString
  - avroSchemaUri, which points to a schema-registry
  - avroSchemaFile
  - jsonSchemaString
  - jsonSchemaFile
* For watermark configuration, the timeField is one field in the dataframe to be used for the delay calculation.
* To add a custom (processing) timestamp, please use the addTimestamp property. This column is added as the name of __timestamp.

If more than one above options are provided, it takes the following order to pick up the schema:
  - avroSchemaString
  - avroSchemaUri
  - avroSchemaFile
  - jsonSchemaString
  - jsonSchemaFile

#### IMPORTANT Note:
   ##### All Kafka built-in columns such as timestamp, partition etc. are renamed to __kafka_*, such as __kafka_timestamp etc.

The definition of the KafkaStreamReader:

- In YAML format
```yaml
  actor:
    type: com.qwshenource.KafkaStreamReader
    properties:
      bootstrapServers: "localhost:9092"
      topic: users
      options:
        startingOffsets: earliest
        endingOffsets: latest
        kafka.group.id: users
        includeHeaders: false
        failOnDataLoss: true
        kafkaConsumer.pollTimeoutMs: 1024
        fetchOffset.numRetries: 3
        fetchOffset.retryIntervalMs: 16
        maxOffsetsPerTrigger: none
      keySchema:
        avroSchemaString: "{\"schema\": \"{\"type\": \"string\"}\"}"
        options:
          timestampFormat: "yyyy-MM-dd'T'HH:mm:ss.sss'Z'"
      valueSchema:
        avroSchemaFile: "schema/users-key.asvc"
        options:
          timestampFormat: "yyyy-MM-dd'T'HH:mm:ss.sss'Z'"
      watermark:
        timeField: event_time
        delayThreshold: 10 seconds
      addTimestamp: true
```
- In JSON format
```json
  {
    "actor": {
      "type": "com.qwshenource.KafkaStreamReader",
      "properties": {
        "bootstrapServers": "localhost:9092",
        "topic": "users",
        "options": {
          "startingOffsets": "earliest",
          "endingOffsets": "latest",
          "kafka.group.id": "users",
          "includeHeaders": false,
          "failOnDataLoss": true,
          "kafkaConsumer.pollTimeoutMs": 1024,
          "fetchOffset.numRetries": 3,
          "fetchOffset.retryIntervalMs": 16,
          "maxOffsetsPerTrigger": "none"
        },
        "keySchema": {
          "avroSchemaUri": "https://user-name:password@localhost:8081"
        },
        "valueSchema": {
          "jsonSchemaFile": "schema/users-key.jschema"
        },
        "watermark": {
          "timeField": "event_time",
          "delayThreshold": "10 seconds"
        },
        "addTimestamp": true
      }
    }
  }
```
- In XML format
```xml
  <actor type="com.qwshen.etl.source.KafkaStreamReader">
    <properties>
      <bootstrapServers>localhost:9092</bootstrapServers>
      <topic>users</topic>
      <options>
        <startingOffsets>earliest</startingOffsets>
        <endingOffsets>latest</endingOffsets>
        <kafka.group.id>users</kafka.group.id>
        <includeHeaders>false</includeHeaders>
        <failOnDataLoss>true</failOnDataLoss>
        <kafkaConsumer.pollTimeoutMs>1024</kafkaConsumer.pollTimeoutMs>
        <fetchOffset.numRetries>3</fetchOffset.numRetries>
        <fetchOffset.retryIntervalMs>16</fetchOffset.retryIntervalMs>
        <maxOffsetsPerTrigger>none</maxOffsetsPerTrigger>
      </options>
      <keySchema>
        <avroSchemaString>{"schema": "{\"type\": \"string\"}"}</avroSchemaString>
        <options>
          <timestampFormat>"yyyy-MM-dd'T'HH:mm:ss.sss'Z'"</timestampFormat>
        </options>
      </keySchema>
      <valueSchema>
        <jsonSchemaString>{"type":"struct","fields":[{"name":"user","type":"string","nullable":true},{"name":"event","type":"string","nullable":true}]}</jsonSchemaString>
        <options>
          <timestampFormat>"yyyy-MM-dd'T'HH:mm:ss.sss'Z'"</timestampFormat>
        </options>
      </valueSchema>
      <watermark>
        <timeField>event_time</timeField>
        <delayThreshold>10 seconds</delayThreshold>
      </watermark>
      <addTimestamp>true</addTimestamp>
    </properties>
  </actor>
```