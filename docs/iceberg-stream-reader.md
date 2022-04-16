The IcebergStreamReader is for reading data from iceberg tables into data-frames in streaming mode with DataFrame API.

- The table of the reading is the full name of an iceberg table.
- The supported read-options are as follows
    - stream-from-timestamp - stream starts from a historical timestamp.
    - streaming-skip-delete-snapshots - flag of whether stream ignores any deleted snapshots.
- For watermark configuration, the timeField is one field in the dataframe to be used for the delay calculation.
- To add a custom (processing) timestamp, please use the addTimestamp property. This column is added as the name of __timestamp.

Please note:
- Iceberg only supports reading data from append snapshots. Overwrite snapshots cannot be processed and will cause an exception. Similarly, delete snapshots will cause an exception by default, but deletes may be ignored by setting streaming-skip-delete-snapshots=true.

Actor Class: `com.qwshen.etl.source.IcebergStreamReader`

The definition of the IcebergStreamReader:

- In YAML format
```yaml
  actor:
    type: iceberg-stream-reader
    properties:
      options:
        stream-from-timestamp: "1650136694"
        streaming-skip-delete-snapshots: "true"
      table: events.db.events
      watermark:
        timeField: event_time
        delayThreshold: 10 seconds
      addTimestamp: true
```
- In JSON format

```json
  {
  "actor": {
    "type": "iceberg-stream-reader",
    "properties": {
      "options": {
        "stream-from-timestamp": "1650136694"
      },
      "table": "events.db.users",
      "watermark": {
        "timeField": "event_time",
        "delayThreshold": "10 seconds"
      },
      "addTimestamp": "true"
    }
  }
}
```
- In XML format
```xml
  <actor type="iceberg-stream-reader">
    <properties>
      <options>
        <stream-from-timestamp>1650136694</stream-from-timestamp>
      </options>
      <table>events.db.users</table>
      <watermark>
        <timeField>event_time</timeField>
        <delayThreshold>10 seconds</delayThreshold>
      </watermark>
      <addTimestamp>true</addTimestamp>
    </properties>
  </actor>
```
