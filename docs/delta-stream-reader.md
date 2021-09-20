The DeltaStreamReader is for reading from delta lake in streaming mode.

- The supported read-options are as follows
    - ignoreDeletes - ignore transactions that delete data at partition boundaries. 
    - ignoreChanges - re-process updates if files had to be rewritten in the source table due to a data changing operation such as UPDATE, MERGE INTO, DELETE (within partitions), or OVERWRITE. Unchanged rows may still be emitted, therefore your downstream consumers should be able to handle duplicates. Deletes are not propagated downstream. ignoreChanges subsumes ignoreDeletes. Therefore if you use ignoreChanges, your stream will not be disrupted by either deletions or updates to the source table.
    - startingVersion - read data with version starting from
    - startingTimestamp - read data with timestamp starting from
    - maxBytesPerTrigger - How much data gets processed in each micro-batch. This option sets a “soft max”, meaning that a batch processes approximately this amount of data and may process more than the limit. If you use Trigger.Once for your streaming, this option is ignored.
    - maxFilesPerTrigger - How many new files to be considered in every micro-batch. The default is 1000.
- The location of the reading can be either by sourcePath or sourceTable. If both specified, sourceTable takes precedence.
- For watermark configuration, the timeField is one field in the dataframe to be used for the delay calculation.
- To add a custom (processing) timestamp, please use the addTimestamp property. This column is added as the name of __timestamp.

The definition of the DeltaStreamReader:

- In YAML format
```yaml
  actor:
    type: delta-stream-reader
    properties:
      options:
        ignoreDeletes: true
        startingVersion: "11"
        startingTimestamp: "2018-09-18T11:15:21.021Z"
        maxBytesPerTrigger: "40960"
        maxFilesPerTrigger: "64"
      sourcePath: /tmp/users
      watermark:
        timeField: event_time
        delayThreshold: 10 seconds
      addTimestamp: true
```
- In JSON format
```json
  {
    "actor": {
      "type": "delta-stream-reader",
      "properties": {
        "options": {
          "ignoreDeletes": "true",
          "ignoreChanges": "true",
          "startingVersion": "11",
          "startingTimestamp": "current_timestamp() - interval 24 hours",
          "maxBytesPerTrigger": "40960",
          "maxFilesPerTrigger": "64"
        },
        "sourceTable": "users",
        "watermark": {
          "timeField": "event_time",
          "delayThreshold": "10 seconds"
        },
        "addTimestamp": true
      }
    }
  }
```
- In XML format
```xml
  <actor type="delta-reader">
    <properties>
      <options>
        <ignoreDeletes>true</ignoreDeletes>
        <ignoreChanges>true</ignoreChanges>
        <startingVersion>11</startingVersion>
        <startingTimestamp>date_sub(current_date(), 1)</startingTimestamp>
        <maxBytesPerTrigger>40960</maxBytesPerTrigger>
        <maxFilesPerTrigger>64</maxFilesPerTrigger>
      </options>
      <sourceTable>users</sourceTable>
      <watermark>
        <timeField>event_time</timeField>
        <delayThreshold>10 seconds</delayThreshold>
      </watermark>
      <addTimestamp>true</addTimestamp>
    </properties>
  </actor>
```