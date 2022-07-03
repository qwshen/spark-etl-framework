The Spark-etl-framework is a pipeline-based data transformation framework using Spark-SQL. For one process flow to transform and move data from end to end, a pipeline is defined. 
At the start of a pipeline, read-actors (readers) are required to load data from the source(s); in the middle of the pipeline, data normally gets transformed with Spark-SQL based transformers;
and finally, at the end of the pipeline, write-actors (writers) write outputs to the target location. A pipeline is not limited to the order of read-transform-write, any of these 3 actions can
be in any stages of a pipeline except write cannot be the first action. Also, custom actors can be developed and plugged-in.


![pipeline flow](docs/images/pipeline.png?raw=true "Pipeline Data Flow")

Technically a pipeline consists of multiple jobs, and each job contains multiple actions which are represented by Actors. Each job can run under the same Spark Session or separate Spark Sub-Sessions; 
and dataframes (as views) can be shared across jobs. Most actors require input view(s) and produce output view(s). 

To build the project, run
```
  mvn clean install -DskipTests
```

### Pipeline definition

The following explains the definition of each section in a pipeline:

#### 1. Settings
  - singleSparkSession - when a pipeline consists of multiple jobs, each job can be executed under a separate Spark Sub-Session. This provides resource isolation across jobs. When this flag is set to true, all jobs are executed under the same global Spark-Session. The default value is false.
  - globalViewAsLocal - global dataframes (views) are shared across jobs (even when they are running under separate Spark Sub-Sessions). Global views are referenced as global_temp.${table}. To make the references easier as local views, set this flag to true. The default value is true.  

  __Please note that variables cannot be referenced in this section.__  
  <br />
  
#### 2. Aliases  
  The aliases section defines the shorthand name used for referencing various actors. However, the alias for each actor must be globally unique. Aliases are optional, if not defined, then the fully-qualified-class-name of an actor must be provided when defining an action for a job.
  ```yaml
  aliases:
    - name: file-reader
      type: com.qwshen.source.FileReader
    - name: sql
      type: com.qwshen.transform.SqlTransformer
    - name: hbase-writer
      type: com.qwshen.sink.HBaseWriter
  ```
  Reference an alias in action definition:
  ```yaml
      actions:
        - name: load users
          actor:
            type: file-reader    # file-reader is an alias for com.qwshen.source.FileReader 
            properties:
              # ...
          output-view:
            name: users
            global: true
  ```
  <u>Please note that variables from application configuration, job-submit arguments can be referenced in this section. However, variables defined inside the current pipeline (see below Variables section) cannot be referenced.</u>      
  <br />
  For centralized and mixed Aliases definition, please check [here](docs/alias.md)  
  <br />

#### 3. UDF-Registration  
  If custom UDFs are required in a pipeline for transforming data, a custom UDF register needs to be provided to register the related UDFs.

  A UDF register must extend com.qwshen.etl.common.UdfRegister, and implement the following method:
  ```scala
  //the prefix is a string for prefixing the name of UDFs in case two UDFs have the same name
  def register(prefix: String)(implicit session: SparkSession): Unit
  ```

  Once the UDF register is implemented, it can be configured at pipeline level as follows:
  ```json
  {
    "udf-registration": [
      {
        "prefix": "event_",
        "type": "com.qwshen.etl.EventUdfRegister"
      },
      {
        "prefix": "user_",
        "type": "com.qwshen.etl.UserUdfRegister"
      }
    ]
  }
  ```

  <u>Please note that variables from application configuration, job-submit arguments can be referenced in this section. However, variables defined inside the current pipeline (see below Variables section) cannot be referenced.</u>      
  <br />
  For centralized and mixed UDF-registration, please check [here](docs/udf-registration.md)  
  For UDF example, please check [here](docs/udf-example.md) 
  <br />

#### 4. Variables  
  The variables defined in this section can be referenced in the definition of the current pipeline, including SQL statements, but not in the Settings, Aliases and Udf-Registration sections  
  - A variable must be given a name and value, and is referenced in the format of ${variable-name}.
  - The value can reference any variables defined in the application configuration, as well as from the job submit arguments. The following example shows that process_date is from events.process_date, which is defined in the application configuration:
    ```xml
    <variables>
        <variable name="process_date" value="${events.process_date}" />
        <variable name="staging_uri" value="concat('/tmp/staging/events', current_date())" />
        <variable name="execution_time" value="now()" />
    </variables>
    ```
    Please Note: a value can also be any valid sql-expression which may reference pre-defined variables. Such as the staging_uri, its value is calculated on the fly when the job starts to run. However, this only applies for the variables defined inside a pipeline. Variables with sql-expression defined in application configuration or from job submit arguments are not evaluated.

  - **When a variable is defined more than once, its value from the job submit arguments overrides what from application configuration, and the value from the Variables section of a pipeline has the highest precedence.**
  - When a variable contains sensitive data, such as a password, its value can be protected by a custom key. The following describes how to encrypt the value, and how to configure the variable:  
    ##### - encrypt the value by running the following command:
      ```shell
      java -cp spark-etl-framework-xxx.jar com.qwshen.Encryptor \
        --key-string ${key-value} --data ${password}
      ```
      If the key is stored in a file:  
      ```shell
      java -cp spark-etl-framework-xxx.jar com.qwshen.Encryptor \
        --key-file ${file-name} --data ${password}
      ```
      The above command will print the encrypted value.  
    ##### - configure the variable:
      ```yaml
       variables:
         - name: db.password
           value: ${events.db.password}
           decryptionKeyString: ${application.security.decryption.key}
      ```
      or
      ```yaml
       variables:
         - name: db.password
           value: ${events.db.password}
           decryptionKeyFile: ${application.security.decryption.keyFile}
      ```
      The ${events.db.password} is the encrypted value from the encryption step.
      <br />

  For variables defined and used inside sql-statements, please check [here](docs/sql-actor.md)
  <br />
  
#### 5. Jobs
  A pipeline may contain multiple jobs while each job may have multiple actions. A job provides a container for resource isolation (when singleSparkSession = false). The output from an action of a job may be shared across actions and across jobs. Each action in a job is represented by an Actor, which is defined by its type, and may have properties for controlling its behavior.
  ```json
  {
    "name": "load features",
    "actor": {
        "type": "delta-reader",
        "properties": {
            "options": {
                "versionAsOf": "0"
            },
            "sourcePath": "${delta_dir}"
        }
    },
    "output-view": {
        "name": "features",
        "global": "false"
    }
  }
  ```
  In above example:
  - The delta-reader is an alias defined in the aliases section pointing to com.qwshe.etl.source.DeltaReader. If alias is not preferred, the fully-qualified class name needs to be specified:
    ```jaon
    "type": "com.qwshe.etl.source.DeltaReader"
    ```
  - The global flag in the output-view determines whether the output of the actor (data-view) can be shared across jobs. Default is false.

  Properties of an actor may reference any variables from application configuration, job-submit arguments and/or defined in the **Variables** section of the current pipeline. Values of the properties can also be any valid sql-expressions which may reference any pre-defined variables.

  Each Actor has at most one output (as a view) which can be referenced in any downstream actors. The view can also be referenced as table in a sql statement:
  ```sql
    select * from features
  ```
  If the view is global (shared across jobs) and globalViewAsLocal = true, the global view can be referenced as a local view like in the above query. Otherwise:
  ```sql
    select * from global_temp.features
  ```  

  <u>The definition of a job is not necessarily embedded in the definition of a pipeline, especially when the job is re-used across multiple pipelines. Instead, the job may be defined in a separated file and included as follows:</u>
  ```yaml
    jobs:
      - include: jobs/job.yaml
  ```
 
#### 6. Metrics Logging
  Metrics collection and logging can be enabled per action based on the following configuration in pipeline definition:
  ```yaml
    metrics-logging:
      uri: "${metrics_uri}"
      actions:
        - load-events
        - transform user-train
  ```
  In the above setting, the metrics of the two actions (load-events, transform user-train) will be collected and written to ${metrics_uri}. The default metrics include schema in DDL format, row-count, estimate-size and execute-time of the views. Custom metrics can be added by following this [guide](docs/custom-actor.md). _Please note: collecting metrics may impact the overall performance._ 
  
#### 7. Staging
  For situations where the results of one or more actions need to be staged for troubleshooting or data verification. This can be achieved by adding the following section in the definition of a pipeline:
  ```yaml
    debug-staging:
      uri: "${staging_uri}"
      actions:
        - load-events
        - transform-user-train
  ```
  In the above setting, the output of the two actions (load-events, transform-user-train) will be staged at ${staging_uri}. _Please note that more actions for staging more impact on the performance_. Therefore, they should be used primarily in dev environments.  
  <br />

### Pipeline examples:
- [template_pipeline.yaml](src/test/resources/pipelines/template_pipeline.yaml) with included [job.yaml](src/test/resources/pipelines/jobs/job.yaml) 
- [template_pipeline.json](src/test/resources/pipelines/template_pipeline.json) with included [job.json](src/test/resources/pipelines/jobs/job.json)
- [template_pipeline.xml](src/test/resources/pipelines/template_pipeline.xml) with included [job.xml](src/test/resources/pipelines/jobs/job.xml)

### Configuration
  One or more application configuration files can be provided when submitting a Spark job. The following is one example:
  ```
  source.events {
    users_input = "data/users"
  }

  kafka {
    bootstrap.servers = "localhost:9092"
    schema.registry.url = "http://localhost:8081"
  }

  application {
    security.decryption.key = "my_secret_123"
    scripts_uri = "./scripts"
  }
  ```
  There are three approaches to providing the runtime configuration:
  ##### 1. Specify runtime variables when submitting a Spark job. See below at **"Submitting a spark-job"**
  ##### 2. Configure runtime variables in application configuration. See the following example:
  ```
  application.runtime {
    spark {
      driver.memory = 16g
      executor.memory = 16g
      serializer = org.apache.spark.serializer.KryoSerializer
      sql {
        extensions = io.delta.sql.DeltaSparkSessionExtension
        catalog.spark_catalog = org.apache.spark.sql.delta.catalog.DeltaCatalog
      }
    }
    
    hadoopConfiguration {
      # skip writing __SUCCESS
      mapreduce.fileoutputcommitter.marksuccessfuljobs = false
    }
    
    # skip writing crc files
    filesystem.skip.write.checksum = true
    # support hive integration
    hiveSupport = true
  }
  ```
  <u>Note: spark configs from application configuration have the highest precedence.</u>
  ##### 3. Use SparkConfActor to dynamically set spark & hadoopConfiguration. For example:
  ```yaml
  - name: set up configuration for s3-access
    actor:
      type: spark-conf-actor
      properties:
        configs:
          spark.sql.shuffle.partitions: 160
        hadoopConfigs:
          fs.s3a.path.style.access: "true"
          fs.s3a.impl: "org.apache.hadoop.fs.s3a.S3AFileSystem"
          fs.s3a.connection.ssl.enabled: "true"
          fs.s3a.endpoint: "s3a.qwshen.com:9000"
          fs.s3a.access.key: "sa_7891"
          fs.s3a.secret.key: "s.UjEjksEnEidFehe\KdenG"
  ```

### Submitting a spark-job
The following is one example of how to submit a Spark job. Note that it also demonstrates how to provide the runtime configs, as well as pass variables.  
```shell
 spark-submit --master yarn|local --deploy-mode client|cluster \
   --name test \
   --conf spark.executor.memory=24g --conf spark.driver.memory=16g \
   --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
   --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
   --jars ./mysql-connector-jar.jar \
   --class com.qwshen.Launcher spark-etl-framework-0.1-SNAPSHOT.jar \
   --pipeline-def ./test.yaml --application-conf ./common.conf,./environment.conf,./application.conf \
   --var process_date=20200921 --var environment=dev \
   --vars encryption_key=/tmp/app.key,password_key=/tmp/pwd.key \
   --staging-uri hdfs://tmp/staging --staging-actions load-events,combine-users-events
```
When multiple config files are provided, configs from the next file override configs from the previous file. In above example, environment.conf overrides common.conf, and application.conf overrides environments.conf.

[Run a live example](docs/submit-job.md), and more [tutorials](docs/tutorials.md)

### Actors
#### - Source readers
- [DeltaReader](docs/delta-reader.md)
- [DeltaStreamReader](docs/delta-stream-reader.md)
- [FileReader](docs/file-reader.md)
- [FileStreamReader](docs/file-stream-reader.md)
- [FlatReader](docs/flat-reader.md)
- [FlatStreamReader](docs/flat-stream-reader.md)
- [HBaseReader](docs/hbase-reader.md)
- [SqlReader](docs/sql-reader.md)
- [SqlTableReader](docs/sql-table-reader.md)
- [JdbcReader](docs/jdbc-reader.md)
- [KafkaReader](docs/kafka-reader.md)
- [KafkaStreamReader](docs/kafka-stream-reader.md)
- [RedisReader](docs/redis-reader.md)
- [RedisStreamReader](docs/redis-stream-reader.md)
- [MongoReader](docs/mongo-reader.md)
- [IcebergReader](docs/iceberg-reader.md)
- [IcebergStreamReader](docs/iceberg-stream-reader.md)

#### - Transformers
- [SqlTransformer](docs/sql-transformer.md)
- [StreamStatefulTransformer](docs/stream-stateful-transformer.md)

#### - Validations
- [SchemaValidator](docs/schema-validator.md)
- [SqlDataValidator](docs/sql-data-validator.md)

#### - Sink writers
- [DeltaWriter](docs/delta-writer.md)
- [DeltaStreamWriter](docs/delta-stream-writer.md)
- [FileWriter](docs/file-writer.md)
- [FileStreamWriter](docs/file-stream-writer.md)
- [HBaseWriter](docs/hbase-writer.md)
- [HBaseStreamWriter](docs/hbase-stream-writer.md)
- [SqlWriter](docs/sl-writer.md)
- [SqlTableWriter](docs/sql-table-writer.md)
- [JdbcWriter](docs/jdbc-writer.md)
- [JdbcStreamWriter](docs/jdbc-stream-writer.md)
- [KafkaWriter](docs/kafka-writer.md)
- [KafkaStreamWriter](docs/kafka-stream-writer.md)
- [RedisWriter](docs/redis-writer.md)
- [RedisStreamWriter](docs/redis-stream-writer.md)
- [MongoWriter](docs/mongo-writer.md)
- [MongoStreamWriter](docs/mongo-stream-writer.md)
- [IcebergWriter](docs/iceberg-writer.md)
- [IcebergStreamWriter](docs/iceberg-stream-writer.md)

#### - Spark-Configuration
- [SparkConfActor](docs/spark-conf-actor.md)

#### - Other Utilities
- [SqlActor](docs/sql-actor.md)
- [VariableSetter](docs/variable-setter.md)
- [ViewPartitioner](docs/view-partitioner.md)

### Writing custom Actor
In cases where the logic of transforming data is very complicated, or a new reader and/or writer would be preferred, a custom Actor can be created by following this [guide](docs/custom-actor.md).

### Appendix: Spark-SQL practices
- Limit the data size by selecting the required columns, adding filtering etc.;
- Never use "in" key word in where clause, such as
  ```SQL
  select * from orders where user_id in (select id from users)
  ```
- Cache tables when needed. The framework checks the number of times of a table referenced and cache the table automatically if the referenced number is greater than 1;
- Always keep eyes on data-skew - this is the #1 troublemaker. 
- Avoid using UDF since it is a black-box and Spark doesn't know how to optimize it;
- Don't write custom Lookup-UDF, instead use broadcast-join.

