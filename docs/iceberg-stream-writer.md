The IcebergStreamWriter is for writing data-frames to iceberg tables in streaming mode.

The table of the writing is the full name of an iceberg table.
- The supported write-options are as follows
    - fanout-enabled - override the target table’s write.spark.fanout.enabled. Default: false. Fanout writer opens the files per partition value and doesn’t close these files till write task is finished. This functionality is encouraged for streaming writes to eliminate the sorting requirements for partitioned tables.
    - checkpointLocation - the location for writing streaming checkpoints.
- The trigger mode must be one of the following values:
  - processingTime - trigger a micro-batch query to start (one micro-batch) by an interval
  - once - trigger the streaming process one time
- The output mode must be one of the following values:
  - complete - all the rows in the streaming DataFrame/Dataset will be written to the sink every time there are some updates.
  - append - only the new rows in the streaming DataFrame/Dataset will be written to the sink.
- The test.waittimeMS is for testing purpose which specify how long the streaming run will be last.

Actor Class: `com.qwshen.etl.sink.IcebergStreamWriter`

The definition of the DeltaStreamWriter:

- In YAML format
```yaml
  actor:
    type: iceberg-stream-writer
    properties:
      table: events.db.features
      options:
        fanout-enabled: "true"
        checkpointLocation: "/tmp/events/features"
      outputMode: complete
      trigger:
        mode: processingTime
        interval: 3 seconds
      test.waittimeMS: 30000
      view: features      
```
- In JSON format
```json
  {
    "actor": {
      "type": "iceberg-stream-writer",
      "properties": {
        "table": "events.db.features",
        "options": {
          "fanout-enabled": "true",
          "checkpointLocation": "/tmp/events/features"
        },
        "outputMode": "append",
        "trigger": {
          "mode": "processingTime",
          "interval": "3 seconds"
        },
        "test.waittimeMS": "30000",
        "view": "features"
      }
    }
  }
```
- In XML format
```xml
  <actor type="delta-writer">
    <properties>
      <table>events.db.features</table>
      <options>
        <fanout-enabled>true</fanout-enabled>
        <checkpointLocation>/tmp/events/features</checkpointLocation>
      </options>
      <trigger>
        <mode>once</mode>
      </trigger>
      <outputMode>append</outputMode>
      <test.waittimeMS>30000</test.waittimeMS>
      <view>features</view>
    </properties>
  </actor>
```
