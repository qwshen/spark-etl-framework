The KafkaStreamWriter is for writing data to Kafka topics in streaming mode.

- If the content of key and/or value already prepared in the source data-frame. Please provide keyField and/or valueField.
- If the schemas of the key/value are provided, the KafkaWriter organizes the content of the key/value according to the schemas
    - avroSchemaString
    - avroSchemaUri, which points to a schema-registry
    - avroSchemaFile
    - jsonSchemaString
    - jsonSchemaFile
- The trigger mode must be one of the following values:
  - continuous
  - processingTime
  - once
- The output mode must be one of the following values:
  - complete
  - append
  - update
- The test.waittimeMS is for testing purpose which specify how long the streaming run will be last.
- The view property specifies which view is to be written to Kafaka.

The definition of the KafkaWriter:

- In YAML format
```yaml
  actor:
    type: com.it21learning.etl.sink.KafkaWriter
    properties:
      bootstrapServers: "localhost:9092"
      topic: users
      options:
        checkpointLocation: /tmp/checkpoint-staging/users
      keyField: user_id
      valueSchema:
        avroSchemaFile: "schema/users-value.asvc"
      trigger:
        mode: continuous
        interval: 3 seconds
      outputMode: append
      test.waittimeMS: 30000
      view: users
```
- In JSON format
```json
  {
    "actor": {
      "type": "com.it21learning.etl.sink.KafkaWriter",
      "properties": {
        "bootstrapServers": "localhost:9092",
        "topic": "users",
        "options": {
          "checkpointLocation": "/tmp/checkpoint-staging/users"
        },
        "keyField": "user_id",
        "valueSchema": {
          "jsonSchemaFile": "schema/users-value.jschema"
        },
        "trigger": {
          "mode": "continuous",
          "interval": "3 seconds"
        },
        "outputMode": "append",
        "test.waittimeMS": "30000",
        "view": "users"
      }
    }
  }
```
- In XML format
```xml
  <actor type="com.it21learning.etl.sink.KafkaWriter">
    <properties>
      <bootstrapServers>localhost:9092</bootstrapServers>
      <topic>users</topic>
      <optioins>
        <checkpointLocation>/tmp/checkpoint-staging/users</checkpointLocation>
      </optioins>
      <keyField>user_id</keyField>
      <valueSchema>
        <jsonSchemaString>{"type":"struct","fields":[{"name":"user","type":"string","nullable":true},{"name":"event","type":"string","nullable":true}]}</jsonSchemaString>
      </valueSchema>
      <trigger>
        <mode>continuous</mode>
        <interval>5 seconds</interval>
      </trigger>
      <outputMode>append</outputMode>
      <test.waittimeMS>30000</test.waittimeMS>
      <view>users</view>
    </properties>
  </actor>
```