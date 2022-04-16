The KafkaStreamWriter is for writing data to Kafka topics in streaming mode.

- If the content of key and/or value already prepared in the source data-frame. Please provide keyField and/or valueField.
- If the schemas of the key/value are provided, the KafkaWriter organizes the content of the key/value according to the schemas
    - avroSchemaString
    - avroSchemaUri, which points to a schema-registry
    - avroSchemaFile
    - jsonSchemaString
    - jsonSchemaFile
- If the key column with name key or keyField not provided, the KafkaStreamWriter generates a unique sequence number per micro-batch
- If the value column with name value or valueField not provided, the KafkaStreamWriter generates json document with all columns in the source dataframe.
- The trigger mode must be one of the following values:
  - continuous - trigger a continuous query to checkpoint by an interval
  - processingTime - trigger a micro-batch query to start (one micro-batch) by an interval
  - once - trigger the streaming process one time
- The output mode must be one of the following values:
  - complete - all the rows in the streaming DataFrame/Dataset will be written to the sink every time there are some updates.
  - append - only the new rows in the streaming DataFrame/Dataset will be written to the sink.
  - update - only the rows that were updated in the streaming DataFrame/Dataset will be written to the sink every time there are some updates.
- The test.waittimeMS is for testing purpose which specify how long the streaming run will be last.
- The view property specifies which view is to be written to Kafka.

Actor Class: `com.qwshen.etl.sink.KafkaStreamWriter`

The definition of the KafkaStreamWriter:

- In YAML format
```yaml
  actor:
    type: com.qwshenink.KafkaWriter
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
      "type": "com.qwshenink.KafkaWriter",
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
  <actor type="com.qwshen.etl.sink.KafkaWriter">
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
      <test>
        <waittimeMS>30000</waittimeMS>
      </test>
      <view>users</view>
    </properties>
  </actor>
```
