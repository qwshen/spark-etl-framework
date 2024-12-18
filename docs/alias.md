#### 1. Aliases can be defined inside a pipeline as follows:
- In Xml:
  ```xml
  <aliases>
      <alias name="file" type="com.qwshen.etl.source.FileReader" />
      <alias name="flat" type="com.qwshen.etl.source.FlatFileReader" />
      <alias name="setting" type="com.qwshen.etl.common.SparkConfActor" />
      <alias name="sql" type="com.qwshen.etl.transform.SqlTransformer" />
  </aliases>
  ```
- In Json
  ```json
  "aliases": [
    {
      "name": "file",
      "type": "com.qwshen.etl.source.FileReader"
    },
    {
      "name": "flat",
      "type": "com.qwshen.etl.source.FlatReader"
    },
    {
      "name": "setting",
      "type": "com.qwshen.etl.setting.SparkConfSetter"
    },
    {
      "name": "sql",
      "type": "com.qwshen.etl.transform.SqlTransformer"
    }
  ]
  ```
- In Yaml
  ```yaml
  aliases:
    - name: file
      type: com.qwshen.etl.source.FileReader
    - name: flat
      type: com.qwshen.etl.source.FlatReader
    - name: setting
      type: com.qwshen.etl.common.SparkConfActor
    - name: sql
      type: com.qwshen.etl.transform.SqlTransformer
  ```
  
#### 2. Besides, aliases can be defined in a centralized file: 
- In Xml. The following content is defined in a separated file - alias.xml
  ```xml
  <aliases>
      <alias name="file" type="com.qwshen.etl.source.FileReader" />
      <alias name="flat" type="com.qwshen.etl.source.FlatFileReader" />
      <alias name="setting" type="com.qwshen.etl.common.SparkConfActor" />
      <alias name="sql" type="com.qwshen.etl.transform.SqlTransformer" />
  </aliases>
  ```
  Then in a pipeline, include above aliases as follows:
  ```xml
  <aliases include="./miscellaneous/alias.xml" />
  ```
  
- In Json. The following aliases are defined in a separated file - alias.json
  ```json
  {
    "aliases": [
      {
        "name": "file",
        "type": "com.qwshen.etl.source.FileReader"
      },
      {
        "name": "flat",
        "type": "com.qwshen.etl.source.FlatReader"
      },
      {
        "name": "setting",
        "type": "com.qwshen.etl.setting.SparkConfSetter"
      },
      {
        "name": "sql",
        "type": "com.qwshen.etl.transform.SqlTransformer"
      }
    ]
  }
  ```
  Then in a pipeline, include above aliases as follows:
  ```json
  "aliases": {
    "include": "./miscellaneous/alias.json"
  }
  ```

- In Yaml. The following aliases are defined in a separated file - alias.yaml
  ```yaml
  aliases:
    - name: file
      type: com.qwshen.etl.source.FileReader
    - name: flat
      type: com.qwshen.etl.source.FlatReader
    - name: setting
      type: com.qwshen.etl.common.SparkConfActor
    - name: sql
      type: com.qwshen.etl.transform.SqlTransformer
  ```
  Then in a pipeline, include above aliases as follows:
  ```yaml
  aliases:
    include: ./miscellaneous/alias.yaml
  ```

#### 3. The mixed mode - for the common aliases that are used across multiple pipelines, the aliases are better defined in a separated file, and include it in each pipeline. However, one pipeline may have a particular alias.
- In Xml. The common aliases are defined in a separated file - alias.xml
  ```xml
  <aliases>
      <alias name="file" type="com.qwshen.etl.source.FileReader" />
      <alias name="flat" type="com.qwshen.etl.source.FlatFileReader" />
      <alias name="sql" type="com.qwshen.etl.transform.SqlTransformer" />
  </aliases>
  ```
  Then in a pipeline, include above aliases with one additional alias as follows:
  ```xml
  <aliases include="./miscellaneous/alias.xml">
    <alias name="setting" type="com.qwshen.etl.common.SparkConfActor" />
  </aliases>
  ```

- In Json. The common aliases are defined in a separated file - alias.json
  ```json
  {
    "aliases": [
      {
        "name": "file",
        "type": "com.qwshen.etl.source.FileReader"
      },
      {
        "name": "flat",
        "type": "com.qwshen.etl.source.FlatReader"
      },
      {
        "name": "sql",
        "type": "com.qwshen.etl.transform.SqlTransformer"
      }
    ]
  }
  ```
  Then in a pipeline, include above aliases with one additional alias as follows:
  ```json
  "aliases": [
    {
      "include": "./miscellaneous/alias.json"
    },
    {
      "name": "setting",
      "type": "com.qwshen.etl.setting.SparkConfSetter"
    }
  ]
  ```

- In Yaml. The common aliases are defined in a separated file - alias.yaml
  ```yaml
  aliases:
    - name: file
      type: com.qwshen.etl.source.FileReader
    - name: flat
      type: com.qwshen.etl.source.FlatReader
    - name: sql
      type: com.qwshen.etl.transform.SqlTransformer
  ```
Then in a pipeline, include above aliases with one additional alias as follows:
  ```yaml
  aliases:
    - include: ./miscellaneous/alias.yaml
    - name: setting
      type: com.qwshen.etl.common.SparkConfActor
  ```
