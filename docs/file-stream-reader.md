The FileStreamReader is for reading files in streaming mode.

- The supported formats include csv, json, avro, parquet, etc.
- The options are optional.
- The schema is optional. If specified, it must be in ddl-schema format. If the schema is defined in a file, please use ddlSchemaFile with the file-name as the value.

- For watermark configuration, the timeField is one field in the dataframe to be used for the delay calculation.
- To add a custom (processing) timestamp, please use the addTimestamp property. This column is added as the name of __timestamp.
- To clean up processed files, please enable cleanSource option.

Actor Class: `com.qwshen.etl.source.FileStreamReader`

The Definition of the FileStreamReader:

- In YAML format
```yaml
  actor:
    type: file-stream-reader
    properties:
      format: csv
      options:
        header: false
        delimiter: ","
        quote: \"
        timestampFormat: "yyyy/MM/dd HH:mm:ss"
      ddlSchemaString: "user_id long, birth_year int, gender string, location string"
      watermark:
        timeField: __timestamp
        delayThreshold: 5 minutes
      addTimestamp: true
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
        "watermark": {
          "timeField": "__timestamp",
          "delayThreshold": "5 minutes"
        },
        "addTimestamp": "true",
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
      <watermark>
        <timeField>__timestamp</timeField>
        <delayThreshold>5 minutes</delayThreshold>
      </watermark>
      <addTimestamp>true</addTimestamp>
      <fileUri>${event.recommendation.data.users.file}</fileUri>
    </properties>
  </actor>
```
