The FlatReader is for reading complex delimited or fixed-length flat files with header and/or trailer from local or hdfs file system.

- The header, body and trailer may be in different format and have different number of fields for each record. The format property must be in one of the following values (with text as the default):
  - delimited
  - fixed-length
  - text

- The header and body may be delimited with different delimiters while the trailer is in fixed-length.

- The identifier of Body can be specified by identifier.matchRgPtn property with a regular-expression pattern; while header can be identified through identifier.beginNRows (first N rows) or identifier.matchRgPtn properties, and trailer through identifier.endNRows (last N rows) or identifier.matchRgPtn properties.
- The dataframe for header/trailer can be accessed through the output-view.name in sql statement.

- For fixed-length flat files, the field-schema, if provided, must be by either ddlFieldsString or ddlFieldsFile, and its content must be in the following format:
    ```
    FieldName1:StartPos1-Length1 FieldType1, FieldName2:StartPos2-Length2 FieldType2, ... 
    ```
    Please note that the position starts from 1 (not 0).

    Example:
    ```
      user:1-8 string, event:9-10 long, timestamp:19-32 string, interested:51-1 int
    ```
    - 1st field: user starting at position of 1 with length of 8 and type of string
    - 2nd field: event starting at position of 9 with length of 10 and type of long
    - 3rd field: timestamp starting at position of 19 with length of 32 and type of string
    - 4th field: interested starting at position of 51 with length of 1 and type of int  
  

- For delimited flat file, the field-schema must be in the following format:
    ```
    FieldName1:index1 FieldType1, FieldName2:index2 FieldType2, ...
    ```
    Please note that the position starts from 0.

    Example:
    ```
      user:0 string, event:1 long, timestamp:5 string, interested:17 int
    ```
    - 1st field: user with index of 0 points to the first field of string type
    - 2nd field: event with index of 1 points to the second field and its type is long
    - 3rd field: timestamp with index of 5 points to the 6-th field  of string type
    - 4th field: interested with index of 17 points to the 18-th field with type of int  


- If the field-schema information is not provided, the resulted dataframe has the following schema:
    ```
     value string
    ```
    To give a custom column name for the value field, please use row.valueField properties in the definition.

- To add a sequence number for each row, please provide the column name for the sequence number by row.noField property.  

- If addInputFile property is enabled, a column called ```___input_file__``` is added in the output dataframe, which indicates which file the current record is from. By default, it is disabled.

- The fallbackRead tells the FlatReader to load an alternative dataframe when the regular load fails. The following properties determine the schema and content of the alternative dataframe:
  - The ddlSchemaString/ddlSchemaFile or ddlFallbackSchemaString/ddlFallbackSchemaFile
  - The fallbackSqlString/fallbackSqlFile

Actor Class: `com.qwshen.etl.source.FlatReader`

The definition of the FlatReader:

- In YAML format
```yaml
  actor:
    type: flat-reader
    properties:
      format: fixed-length
      identifier:
        matchRgPtn: "^Dta"
      ddlFieldsString: "user:1-8 string, event:9-10 long, timestamp:19-32 string, interested:51-1 int"
      header:
        format: fixed-length
        identifier:
          beginNRows: 1
        ddlFieldsString: "id:1-3 string, data_date:4-8 string"
        output-view:
          name: "user_header"
          global: false
      trailer:
        format: fixed-length
        identifier:
          matchRgPtn: "^TRL"
        ddlFieldsString: "id:1-3 string, rec_count:4-7 int"
        output-view:
          name: "user_trailer"
          global: true
      addInputFile: true
      row:
        noField: seq_no
      fileUri: "${events.train_input}"
      multiUriSeparator: ";"
      output-view:
        name: train
        global: true
```
or
```yaml
  actor:
    type: flat-reader
    properties:
      format: delimited
      options:
        header: false
        delimitor: |
      identifier:
        matchRgPtn: "^Dta"
      ddlFieldsString: "user:0 string, event:1 long, timestamp:5 string, interested:11 int"
      header:
        format: delimited
        options:
          delimiter: ,
        identifier:
          beginNRows: 1
        ddlFieldsString: "id:0 string, data_date:1 string"
        output-view:
          name: "user_header"
          global: false
      trailer:
        format: fixed-length
        identifier:
          matchRgPtn: "^TRL"
        ddlFieldsString: "id:1-3 string, rec_count:4-7 int"
        output-view:
          name: "user_trailer"
          global: true
      addInputFile: true
      fileUri: "${events.train_input}"
      multiUriSeparator: ";"
      output-view:
        name: train
        global: true
```

- In JSON format
```json
  {
    "actor": {
      "type": "flat-reader",
      "properties": {
        "format": "text",
        "row": {
          "noField": "key",
          "valueField": "text"
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
        "addInputFile": "false",
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
