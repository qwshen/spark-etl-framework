The DeltaReader is for reading from delta lake in batch mode.

- The supported read-options are as follows
    - timestampAsOf - to specify the timestamp of data to be read from data lake
    - versionAsOf - to specify the version of data to be read from data lake
- The location of the reading can be either by sourcePath or sourceTable. If both specified, sourceTable takes precedence.

Actor Class: `com.qwshen.etl.source.DeltaReader`

The definition of the DeltaReader:

- In YAML format
```yaml
  actor:
    type: delta-reader
    properties:
      options:
        timestampAsOf: "2018-09-18T11:15:21.021Z"
        versionAsOf: "11"
      sourcePath: /tmp/users
```
- In JSON format
```json
  {
    "actor": {
      "type": "delta-reader",
      "properties": {
        "options": {
          "timestampAsOf": "2018-09-18T11:15:21.021Z",
          "versionAsOf": "11"
        },
        "sourceTable": "users",
      }
    }
  }
```
- In XML format
```xml
  <actor type="delta-reader">
    <properties>
      <options>
        <timestampAsOf>2018-09-18T11:15:21.021Z</timestampAsOf>
        <versionAsOf>11</versionAsOf>
      </options>
      <sourceTable>users</sourceTable>
    </properties>
  </actor>
```
