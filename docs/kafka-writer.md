The KafkaWriter is for writing data to Kafka topics in batch mode.

- If the content of key and/or value already prepared in the source data-frame. Please provide keyField and/or valueField.
- If the schemas of the key/value are provided, the KafkaWriter organizes the content of the key/value according to the schemas
  - avroSchemaString
  - avroSchemaUri, which points to a schema-registry
  - avroSchemaFile
  - jsonSchemaString
  - jsonSchemaFile
- If the key column with name key or keyField not provided, the KafkaWriter generates a unique sequence number per batch
- If the value column with name value or valueField not provided, the KafkaWriter generates json document with all columns in the source dataframe.

The definition of the KafkaWriter:

- In YAML format
```yaml
  actor:
    type: com.it21learning.etl.sink.KafkaWriter
    properties:
      bootstrapServers: "localhost:9092"
      topic: users
      keyField: user_id
      valueSchema:
        avroSchemaFile: "schema/users-value.asvc"
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
        "keyField": "user_id",
        "valueSchema": {
          "jsonSchemaFile": "schema/users-value.jschema"
        }
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
      <keyField>user_id</keyField>
      <valueSchema>
        <jsonSchemaString>{"type":"struct","fields":[{"name":"user","type":"string","nullable":true},{"name":"event","type":"string","nullable":true}]}</jsonSchemaString>
      </valueSchema>
    </properties>
  </actor>
```