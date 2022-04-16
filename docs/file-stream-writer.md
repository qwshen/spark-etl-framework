The FileStreamWriter is for writing a data-frame a target file system in streaming mode.

- The support formats are csv, json, avro & parquet.
- The partition-by is optional. If provided, it must be the names of one or more columns separated by comma.
- The checkpointLocation can be specified as one write-option.
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

Actor Class: `com.qwshen.etl.sink.FileStreamWriter`

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
