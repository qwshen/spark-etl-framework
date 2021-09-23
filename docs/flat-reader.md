The FlatReader is for reading flat files from local or hdfs file system.

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

If the field-schema information is not provided, the resulted dataframe has the following schema:
```dtd
   row_no int, row_value string, input_file string
```
To give a custom column name for row_no and/or row_value, please use row.noField and/or row.valueField properties in the definition.

The definition of the FlatReader:

- In YAML format
```yaml
  actor:
    type: flat-reader
    properties:
      options:
        header: true
      ddlFieldsString: "user:1-8 string, event:9-10 long, timestamp:19-32 string, interested:51-1 int" 
      fileUri: "${events.train_input}" 
```
or
```yaml
  actor:
    type: flat-reader
    properties:
      ddlFieldsFile: schema/train.txt 
      fileUri: "${events.train_input}" 
```

- In JSON format
```json
  {
    "actor": {
      "type": "flat-reader",
      "properties": {
        "row": {
          "noField": "key",
          "valueField": "value"
        },
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
      <fileUri>${events.train_input}</fileUri>
    </properties>
  </actor>
```
or 
```xml
  <actor type="flat-reader">
    <properties>
      <ddlFieldsFile>schema/train.txt</ddlFieldsFile>
      <fileUri>${events.train_input}</fileUri>
    </properties>
  </actor>
```