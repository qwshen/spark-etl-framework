The FileStreamWriter is for writing a data-frame a target file system in streaming mode.

- The support formats are csv, json, avro & parquet.
- The partition-by is optional. If provided, it must be the names of one or more columns separated by comma.
- The checkpointLocation can be specified as one write-option.
- The trigger mode must be one of the following values:
    - continuous
    - processingTime
    - once
- The output mode must be one of the following values:
    - complete
    - append
    - update
- The test.waittimeMS is for testing purpose which specify how long the streaming run will be last.
- The view property specifies which view is to be written to Kafka.

The definition of the FileStreamWriter:

- In YAML format
```yaml
  actor:
    type: file-writer
    properties:
      format: csv
      options:
        header: true
        checkpointLocation: /tmp/checkpoint
      partitionBy: "gender,birthyear"
      trigger:
        mode: continuous
        interval: 3 seconds
      outputMode: append
      test.waittimeMS: 30000
      fileUri: "${export_dir}"
      view: features
```

- In JSON format
```json
  {
    "actor": {
      "type": "file-writer",
      "properties": {
        "format": "csv",
        "options": {
          "header": true,
          "checkpointLocation": "/tmp/checkpoint"
        },
        "partitionBy": "gender,birthyear",
        "trigger": {
          "mode": "processingTime",
          "interval": "3 seconds"
        },
        "outputMode": "append",
        "test": {
          "waittimeMS": "3000"
        },
        "fileUri": "${export_dir}",
        "view": "features"
      }
    }
  }
```

- In XML format
```xml
  <actor type="file-writer">
    <properties>
      <format>csv</format>
      <options>
        <header>true</header>
        <maxRecordsPerFile>30000</maxRecordsPerFile>
      </options>
      <partitionBy>gender,birthyear</partitionBy>
      <trigger>
        <mode>continuous</mode>
        <interval>5 seconds</interval>
      </trigger>
      <outputMode>append</outputMode>
      <test>
        <waittimeMS>30000</waittimeMS>
      </test>
      <fileUri>${export_dir}</fileUri>
      <view>features</view>
    </properties>
  </actor>
```