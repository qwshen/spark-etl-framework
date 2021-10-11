- Create a class by inheriting com.qwshen.etl.common.Actor
```scala
package com.hello.components

class MyActor extends com.it21learning.etl.common.Actor {}
```
- Define the properties
```scala
import com.it21learning.common.PropertyKey

class MyActor extends com.it21learning.etl.common.Actor {
  @PropertyKey("options.*", false)
  private var _options: Map[String, String] = Map.empty[String, String]

  @PropertyKey("sourcePath", true)
  private var _sourcePath: Option[String] = None

  //.....
}
```
With the following definition in a pipeline, the above properties will be populated by the framework automatically:
```yaml
actor:
  - name: the work for my-actor
    type: com.hello.components.MyActor
    properties:
      options:
        prop1: val1
        prop2: val2
      sourcePath: /tmp/data/my-customers
```
So, in this case, the _options property will hold a map of (prop1 -> val1, prop2 -> val2), and the _sourcePath will have the value of /tmp/data/my-customers.  
If the _sourcePath is not provided a value in the pipeline definition, a runtime error will be thrown since this property is **required**.

If custom logic needs to be handled during the initialization, override the following method with custom implementation:
```scala
  override def init(properties: Seq[(String, String)], config: Config)(implicit session: SparkSession): Unit = {
    super.init(properties, config)

    //custom implementation here
  }
```
Make sure the **super.init(properties, config)** is call at the beginning of the custom implementation.

- implement the data read/write/transformation logic:
```scala
  def run(ctx: ExecutionContext)(implicit session: SparkSession): Option[DataFrame] = {
    //custom implementation here
  }
```
