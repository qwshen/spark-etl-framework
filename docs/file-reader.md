The FileReader is for loading files from various locations, such as local, hdfs file-system. 

- The supported formats include csv, json, avro, parquet, etc.
- The options are optional.
- The schema is optional. If specified, it must be in ddl-schema format. If the schema is defined in a file, please use ddlSchemaFile with the file-name as the value.

The Definition of the FileReader:

- In YAML format
```yaml
  actor:
    type: file
    properties:
      format: csv
      options:
        header: false
        delimiter: ","
        quote: \"
        timestampFormat: "yyyy/MM/dd HH:mm:ss"
      ddlSchemaString: "user_id long, birth_year int, gender string, location string"
      fileUri: "${event.recommendation.data.users.file}"
```

- In JSON format
```json
  {
    "actor": {
      "type": "file",
      "properties": {
        "format": "csv",
        "options": {
          "header": false,
          "delimiter": ",",
          "quote": "\"",
          "timestampFormat": "yyyy/MM/dd HH:mm:ss"
        },
        "ddlSchemaString": "user_id long, birth_year int, gender string, location string",
        "fileUri": "${event.recommendation.data.users.file}"
      }
    }
  }
```
- In XML format
```xml
  <actor type="file">
    <properties>
      <format>csv</format>
      <options>
        <header>false</header>
          <delimiter>,</delimiter>
          <quote>"</quote>
          <timestampFormat>yyyy/MM/dd HH:mm:ss</timestampFormat>
      </options>
      <ddlSchemaString>user_id long, birth_year int, gender string, location string</ddlSchemaString>
      <fileUri>${event.recommendation.data.users.file}</fileUri>
    </properties>
  </actor>
```