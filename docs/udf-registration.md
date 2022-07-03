A custom UDF must be registered before it can be used in actors or sql-statements:
####1. Directly inside pipeline definition:
- In Xml:
  ```xml
  <udf-registration>
    <register prefix="event_" type="com.qwshen.etl.test.udf.EventUdf" />
    <register prefix="user_" type="com.qwshen.etl.test.udf.UserUdf" />
  </udf-registration>
  ```
- In Json
  ```json
  "udf-registration": [
    {
      "prefix": "event_",
      "type": "com.qwshen.etl.test.udf.EventUdf"
    },
    {
      "prefix": "user_",
      "type": "com.qwshen.etl.test.udf.UserUdf"
    }
  ]
  ```
- In Yaml
  ```yaml
  udf-registration:
    - prefix: event_
      type: com.qwshen.etl.test.udf.UserUdf
    - prefix: user_
      type: com.qwshen.etl.test.udf.UserUdf
  ```

####2. Besides, udf-registratioin can be defined in a centralized file:
- In Xml. The following content is defined in a separated file - udf-registration.xml
  ```xml
  <udf-registration>
    <register prefix="event_" type="com.qwshen.etl.test.udf.EventrUdf" />
    <register prefix="user_" type="com.qwshen.etl.test.udf.UserUdf" />
  </udf-registration>
  ```
  Then in a pipeline, include above udf-registration as follows:
  ```xml
  <udf-registration include="./miscellaneous/udf-registration.xml" />
  ```

- In Json. The following udf-registration is defined in a separated file - udf-registration.json
  ```json
  {
    "udf-registration": [
      {
        "prefix": "event_",
        "type": "com.qwshen.etl.test.udf.EventUdf"
      },
      {
        "prefix": "user_",
        "type": "com.qwshen.etl.test.udf.UserUdf"
      }
    ]
  }
  ```
  Then in a pipeline, include above udf-registration as follows:
  ```json
  "udf-registratioin": {
    "include": "./miscellaneous/udf-registration.json"
  }
  ```

- In Yaml. The following udf-registration is defined in a separated file - udf-registration.yaml
  ```yaml
  udf-registration:
    - prefix: event_
      type: com.qwshen.etl.test.udf.UserUdf
    - prefix: user_
      type: com.qwshen.etl.test.udf.UserUdf
  ```
  Then in a pipeline, include above udf-registration as follows:
  ```yaml
  udf-registration:
    include: ./miscellaneous/udf-registration.yaml
  ```

####3. The mixed mode - for the common UDFs that are used across multiple pipelines, the udf-registration is better defined in a separated file, and include it in each pipeline. However, one pipeline may have a particular udf-registration.
- In Xml. The common udf-registration is defined in a separated file - udf-registration.xml
  ```xml
  <udf-registration>
    <register prefix="event_" type="com.qwshen.etl.test.udf.EventUdf" />
  </udf-registration>
  ```
  Then in one pipeline, include above udf-registration with one additional udf-registration as follows:
  ```xml
  <udf-registration include="./miscellaneous/udf-registration.xml">
    <register prefix="user_" type="com.qwshen.etl.test.udf.UserUdf" />
  </udf-registration>
  ```

- In Json. The common udf-registration is defined in a separated file - udf-registration.json
  ```json
  {
    "udf-registration": [
      {
        "prefix": "event_",
        "type": "com.qwshen.etl.test.udf.UserUdf"
      }
    ]
  }
  ```
  Then in a pipeline, include above udf-registration with one additional udf-registration as follows:
  ```json
  "udf-registration": [
    {
      "include": "./miscellaneous/udf-registratioin.json"
    },
    {
      "prefix": "user_",
      "type": "com.qwshen.etl.test.udf.UserUdf"
    }
  ]
  ```

- In Yaml. The common udf-registration is defined in a separated file - udf-registration.yaml
  ```yaml
  udf-registration:
    - prefix: event_
      type: com.qwshen.etl.test.udf.EventUdf
  ```
  Then in a pipeline, include above udf-registration with one additional udf-registration as follows:
  ```yaml
  udf-registration:
    - include: ./miscellaneous/udf-registratioin.yaml
    - prefix: user_
      type: com.qwshen.etl.test.udf.UserUdf
  ```

For UDF example, please check [here](./udf-example.md)  
