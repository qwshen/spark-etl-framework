The BinaryFileReader is for reading complex delimited or fixed-length binary files with header and/or trailer from local or hdfs file system.

- The BinaryFileReader is from [FlatFileReader](./flat-file-reader.md) with addition of binary decoding which can be defined at
  - row level
  ```yaml
    properties:
      recordLength: 1024
      row:
        transformation: "bytes_to_string($., 'ISO-8859-15')"
  ```
  - field level 
  ```json
  {
    "properties": {
      "fieldTransformation": {
        "default": "bytes_to_string($., 'ISO-8859-15')",
        "x_amount": "com3_to_double(x_amount)",
        "x_date": "bytes_to_string(substring($., 3, 8), 'ISO-8859-15')"
      }
    }
  }
  ```
  Note:  
    - The ```row.transformation``` once defined, is applied to every row just after dataframe loaded.
    - The ```fieldTransformation``` is applied to each individual field after each row gets split. 
      - The default transformation is applied to all fields except configured otherwise. The $. variable represents the field itself, so for field customer_name, it is tranformed as ```bytes_to_string(customer_name, 'ISO-8859-15')```
      - For the field x_date, the $. variable represents the original raw-row.
    - Both ```row.transformation``` and ```fieldTransformation``` are optional.
      <br />
      <br />

- The parsing process of BinaryFileReader is as follows:
  - load raw data into a dataframe. If ```recordLength``` is specified, bytes from file(s) are split by this length into records, otherwise bytes from each file is one record.
  - apply ```row.transformation``` if provided.
  - extract header and trailer if they are defined.
  - split records by delimiter if provided. It needs to be aware of the ```row.transformation``` when defining the delimiter.
    - delimiter must be in byte value if ```row.transformation``` not defined. For example with (,) as delimiter:
      ```yaml
      options:
        delimiter: "0x6b"  
      ``` 
      or 
      ```yaml
      options:
        delimiter: "107"  
      ``` 
      If delimiter contains multiple bytes, separate them by (,), like ```"0x6b,57"```.
    - delimiter must be in text. For example:
      ```yaml
      options:
        delimiter: ","  
      ``` 
  - apply ```fieldTransformation```

Actor Class: `com.qwshen.etl.source.BinaryFileReader`

The definition of the BinaryFileReader:

- In YAML format
```yaml
- name: load train_csv_without_rowTransformatioin
  actor:
    type: binary-reader
    properties:
      row:
        noField: seq_number
      recordLength: 57
      header:
        identifier:
          matchExpr: "bytes_to_string(substring($., 1, 3), 'ISO-8859-15') = 'HDR'"
        format: delimited
        options:
          header: false
          delimiter: "0x6b"
        ddlFieldsString: "id:0 string, data_date:1 string"
        fieldTransformation:
          default: "bytes_to_string($., 'ISO-8859-15')"
        output-view:
          name: train_header_csv
          global: true
      format: delimited
      options:
        header: false
        delimiter: "107"
      ddlFieldsString: "user:0 string, event:1 string, timestamp:3 string, interested:4 string"
      fieldTransformation:
        default: "bytes_to_string($., 'ISO-8859-15')"
      trailer:
        identifier:
          endNRows: 1
        format: delimited
        options:
          header: false
          delimiter: "0x4f"
        ddlFieldsString: "id:0 string, rec_count:1 string"
        fieldTransformation:
          default: "bytes_to_string($., 'ISO-8859-15')"
          rec_count: "cast(trim(bytes_to_string(rec_count, 'ISO-8859-15')) as int)"
        output-view:
          name: train_trailer_csv
      addInputFile: true
      fileUri: ${events.train_input}/train_csv.bin
    output-view:
      name: train_csv
```
or
```yaml
- name: load train_fixed_length_with_rowTransformation
  actor:
    type: binary-reader
    properties:
      recordLength: 52
      row:
        transformation: "bytes_to_string($., 'cp037')"
      header:
        identifier:
          matchRegex: "^HDR"
        format: fixed-length
        ddlFieldsString: "id:1-3 string, data_date:4-8 string"
        output-view:
          name: train_header
          global: true
      format: fixed-length
      ddlFieldsString: "user:1-9 string, event:10-10 long, timestamp:20-32 string, interested:52-1 int"
      trailer:
        identifier:
          endNRows: 1
        format: fixed-length
        ddlFieldsString: "id:1-3 string, rec_count:4-7 string"
        fieldTransformation:
          rec_count: "com3_to_int(substring($., 4, 7))"
        output-view:
          name: train_trailer
      fileUri: ${events.train_input}/train_txt.bin
    output-view:
      name: train
```
or
```yaml
- name: load train_fixed_length_without_rowTransformation
  actor:
    type: binary-reader
    properties:
      recordLength: 52
      header:
        identifier:
          matchExpr: "bytes_to_string(substring($., 1, 3), 'cp037') = 'HDR'"
        format: fixed-length
        ddlFieldsString: "id:1-3 string, data_date:4-8 string"
        fieldTransformation:
          default: "bytes_to_string($., 'cp037')"
        output-view:
          name: train_header
          global: true
      format: fixed-length
      ddlFieldsString: "user:1-9 string, event:10-10 long, timestamp:20-32 string, interested:52-1 int"
      fieldTransformation:
        default: "bytes_to_string($., 'cp037')"
      trailer:
        identifier:
          endNRows: 1
        format: fixed-length
        ddlFieldsString: "id:1-3 string, rec_count:4-7 string"
        fieldTransformation:
          default: "bytes_to_string($., 'cp037')"
          rec_count: "com3_to_int(rec_count)"
        output-view:
          name: train_trailer
      fileUri: ${events.train_input}/train_txt.bin
    output-view:
      name: train
```
or
```yaml
- name: load train_csv_with_rowTransformatioin
  actor:
    type: binary-reader
    properties:
      recordLength: 57
      row:
        noField: seq_number
        transformation: "bytes_to_string($., 'cp037')"
      header:
        identifier:
          matchRegex: "^HDR"
        format: delimited
        options:
          header: false
          delimiter: ","
        ddlFieldsString: "id:0 string, data_date:1 string"
        output-view:
          name: train_header_csv
          global: true
      format: delimited
      options:
        header: false
        delimiter: ","
      ddlFieldsString: "user:0 string, event:1 string, timestamp:3 string, interested:4 string"
      trailer:
        identifier:
          endNRows: 1
        format: delimited
        options:
          header: false
          delimiter: "|"
        ddlFieldsString: "id:0 string, rec_count:1 string"
        fieldTransformation:
          rec_count: "cast(trim(rec_count) as int)"
        output-view:
          name: train_trailer_csv
      addInputFile: true
      fileUri: ${events.train_input}/train_csv.bin
    output-view:
      name: train_csv
```

- In JSON format
```json
  {
    "actor": {
      "type": "binary-reader",
      "properties": {
        "row": {
          "noField": "key",
          "valueField": "data"
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
      "type": "binary-reader",
      "properties": {
        "format": "delimited",
        "ddlFieldsFile": "schema/train.txt",
        "addInputFile": "false",
        "fileUri": "${events.train_input}"
      }
    }
  }
```

- In XML format
```xml
  <actor type="binary-reader">
    <properties>
      <format>fixed-length</format>
      <ddlFieldsString>user:1-8 binary, event:9-10 long, timestamp:19-32 string, interested:51-1 int</ddlFieldsString>
      <fileUri>${events.train_input}</fileUri>
    </properties>
  </actor>
```
or
```xml
  <actor type="binary-reader">
  <properties>
    <format>delimited</format>
    <ddlFieldsFile>schema/train.txt</ddlFieldsFile>
    <fileUri>${events.train_input}</fileUri>
  </properties>
</actor>
```
or
```xml
  <actor type="binary-reader">
  <properties>
    <fileUri>${events.train_input}</fileUri>
  </properties>
</actor>
```
