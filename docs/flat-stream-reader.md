The FlatStreamReader is for reading flat files a file system in streaming mode.

The options are optional.

- maxFilesPerTrigger - the max # of files processed per micro-batch.

The field-schema information optional. If provided, it must be by either ddlFieldsString or ddlFieldsFile, and its content must be in the following format:

- FieldName1:StartPos1-Length1 FieldType1, FieldName2:StartPos2-Length2 FieldType2, ...

Please note that the position starts from 1 (not 0).

Example:
```dtd
    user:1-8 string, event:9-10 long, timestamp:19-32 string, interested:51-1 int
```
- 1st field: user starting at position of 1 with length of 8 and type of string
- 2nd field: event starting at position of 9 with length of 10 and type of long
- 3rd field: timestamp starting at position of 19 with length of 32 and type of string
- 4th field: interested starting at position of 51 with length of 1 and type of int

If the field-schema information is not provided, the output dataframe has the following schema:
```dtd
   row_value string
```
To give a custom column name for the row_value, please use row.valueField properties in the definition.

For watermark configuration, the timeField is one field in the dataframe to be used for the delay calculation.
To add a custom (processing) timestamp, please use the addTimestamp property. This column is added as the name of __timestamp.

To clean up processed files, please enable cleanSource option.

Actor Class: `com.qwshen.etl.source.FlatStreamReader`

The definition of the FlatReader:

- In YAML format
```yaml
  actor:
    type: flat-reader
    properties:
      options:
        header: true,
        maxFilesPerTrigger: 5
      ddlFieldsString: "user:1-8 string, event:9-10 long, timestamp:19-32 string, interested:51-1 int"
      watermark:
        timeField: __timestamp
        delayThreshold: 5 minutes
      addTimestamp: true
      fileUri: "${events.train_input}" 
```
or
```yaml
  actor:
    type: flat-reader
    properties:
      ddlFieldsFile: schema/train.txt
      watermark:
        timeField: __timestamp
        delayThreshold: 5 minutes
      addTimestamp: true
      fileUri: "${events.train_input}" 
```

- In JSON format
```json
  {
    "actor": {
      "type": "flat-reader",
      "properties": {
        "row": {
          "valueField": "value"
        },
        "watermark": {
          "timeField": "__timestamp",
          "delayThreshold": "5 minutes"
        },
        "addTimestamp": true,
        "fileUri": "${events.train_input}"
      }
    }
  }
```
or
```json
  {
    "actor": {
      "type": "flat-reader",
      "properties": {
        "ddlFieldsFile": "schema/train.txt",
        "watermark": {
          "timeField": "__timestamp",
          "delayThreshold": "5 minutes"
        },
        "addTimestamp": true,
        "fileUri": "${events.train_input}"
      }
    }
  }
```

- In XML format
```xml
  <actor type="flat-reader">
    <properties>
      <ddlFieldsString>user:1-8 string, event:9-10 long, timestamp:19-32 string, interested:51-1 int</ddlFieldsString>
      <row>
        <valueField>value</valueField>
      </row>
      <watermark>
        <timeField>__tiemstamp</timeField>
        <delayThreshold>5 minutes</delayThreshold>
      </watermark>
      <addTimestamp>true</addTimestamp>
      <fileUri>${events.train_input}</fileUri>
    </properties>
  </actor>
```
or
```xml
  <actor type="flat-reader">
    <properties>
      <ddlFieldsFile>schema/train.txt</ddlFieldsFile>
      <watermark>
        <timeField>__tiemstamp</timeField>
        <delayThreshold>5 minutes</delayThreshold>
      </watermark>
      <addTimestamp>true</addTimestamp>
      <fileUri>${events.train_input}</fileUri>
    </properties>
  </actor>
```
