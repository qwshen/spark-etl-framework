#### 1. Create a class by inheriting from com.qwshen.etl.common.Actor
```scala
package com.hello.components

class MyActor extends com.qwshen.etl.common.Actor {}
```

#### 2. Define the properties
```scala
import com.qwshen.common.PropertyKey

//For Scala 2.11
class MyActor extends com.qwshen.etl.common.Actor {
  @PropertyKey("options.*", false)
  protected var _options: Map[String, String] = Map.empty[String, String]

  @PropertyKey("sourcePath", true)
  protected var _sourcePath: Option[String] = None

  //.....
}

//For Scala 2.12 and above
class MyActor extends com.qwshen.etl.common.Actor {
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

#### 3. If custom logic needs to be handled during the initialization, override the following method:
```scala
  override def init(properties: Seq[(String, String)], config: Config)(implicit session: SparkSession): Unit = {
    super.init(properties, config)

    //custom implementation here
  }
```
Make sure the **super.init(properties, config)** is called at the beginning of the method.

#### 4. Implement the data read/write/transformation logic:
```scala
  def run(ctx: JobContext)(implicit session: SparkSession): Option[DataFrame] = {
    //custom implementation here
  }
```
The following code is to retrieve an existing view by name:
```scala
  @PropertyKey("view", true)
  private var _view: Option[String] = None

  def run(ctx: JobContext)(implicit session: SparkSession): Option[DataFrame] = for {
    //...
    df <- this._view.flatMap(name => ctx.getView(name))
    //...
  } yield {
    //custom implementation here
  }
```

#### 5. If custom metrics need to be added during the execution of the actor, override the following method:
```scala
def collectMetrics(df: DataFrame)(implicit session: SparkSession): Seq[(String, String)] = {
  //custom implementation here
}
```
If preparation of collecting metrics is required in the execution of the actor, use the following hint
```scala
  def run(ctx: JobContext)(implicit session: SparkSession): Option[DataFrame] = {
    if (ctx.metricsRequired) {
      //prepare for metrics collection
    }
  }
```
_Please note: custom metrics are only collected when the actor has metric-logging enabled in the pipeline definition._
