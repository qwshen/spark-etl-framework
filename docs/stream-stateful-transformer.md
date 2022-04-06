The StreamStatefulTransformer is for running a custom arbitrary stateful transformation with mapGroupsWithState or flatMapGroupsWithState. A custom ArbitraryStatefulProcessor must be provided with
the implementation of the stateful transformation. The ArbitraryStatefulProcessor defines the following method:
```scala
def transformState(df: DataFrame): DataFrame
```
The StreamStatefulTransformer requires an input view against which to execute the stateful transformation. 

Actor Class: `com.qwshen.etl.transform.StreamStatefulTransformer`

The definition of the StreamStatefulTransformer:
- in YAML
```yaml
  actor:
    type: com.qwshen.etl.transform.StreamStatefulTransformer
    properties:
      processor:
        type: a custom-ArbitraryStatefulProcessor
        # properties of the custom processor
      view: users
```
- in Json
```json
  {
    "actor": {
      "type": "com.qwshen.etl.transform.StreamStatefulTransformer",
      "properties": {
        "processor": {
          "type": "a custom-ArbitraryStatefulProcessor",
          "_comments": "properties of the custom processor"
        },
        "view": "users"
      }
    }
  }
```
- in XML
```xml
  <actor type="com.qwshen.etl.transform.StreamStatefulTransformer">
    <properties>
      <processor>
        <type>a custom-ArbitraryStatefulProcessor</type>
        <!-- properties of the custom processor -->
      </processor>
      <view>users</view>
    </properties>
  </actor>
```

This [user-stateful-processor](../src/test/scala/com/qwshen/etl/test/stream/UserStatefulProcessor.scala) is one example of the ArbitraryStatefulProcessor with its states - 
[AgeState](../src/test/scala/com/qwshen/etl/test/stream/AgeState.scala), [InputUser](../src/test/scala/com/qwshen/etl/test/stream/InputUser.scala) & [OutputAge](../src/test/scala/com/qwshen/etl/test/stream/OutputAge.scala).
