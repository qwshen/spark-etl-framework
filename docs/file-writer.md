The FileWriter is for writing a data-frame to files in a local or hdfs file system.

- The support formats are csv, json, avro & parquet.
- The write mode can only be overwrite or append
- The partition-by is optional. If provided, it must be the names of one or more columns separated by comma.
- The empty-write is optional, which controls whether to write out an empty view. It must be either yes/no or disabled/enabled. Default: yes.

Actor Class: `com.qwshen.etl.sink.FileWriter`

The definition of the FileWriter:

- In YAML format
```yaml
  actor:
    type: file-writer
    properties:
      emptyWrite: "no"
      format: csv
      options:
        header: true
        maxRecordsPerFile: 30000
      partitionBy: "gender,birthyear"
      mode: overwrite
      fileUri: "${export_dir}"
      view: features
```

- In JSON format
```json
  {
    "actor": {
      "type": "file-writer",
      "properties": {
        "emptyWrite": "yes",
        "format": "csv",
        "options": {
          "header": true,
          "maxRecordsPerFile": 16
        },
        "partitionBy": "gender,birthyear",
        "mode": "overwrite",
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
      <emptyWrite>disabled</emptyWrite>  
      <format>csv</format>
      <options>
        <header>true</header>
        <maxRecordsPerFile>30000</maxRecordsPerFile>
      </options>
      <partitionBy>gender,birthyear</partitionBy>
      <mode>overwrite</mode>
      <fileUri>${export_dir}</fileUri>
      <view>features</view>
    </properties>
  </actor>
```
