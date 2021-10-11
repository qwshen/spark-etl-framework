The Spark-etl-framework is a pipeline based data transformation framework using Spark-SQL. For one process to transform & move data from end to end, a pipeline needs to be defined,
and as a start of a pipeline, one or more readers required to load data from the source(s), then the data gets transformed in the middle by using Spark-SQL (extensible 
with custom components), finally at the end (not necessarily) of the pipeline, writer(s) write(s) out the result to target storage.

![pipeline flow](docs/images/pipeline.png?raw=true "Pipeline Data Flow")

A pipeline consists of multiple jobs, and each job contains multiple actions (each is represented by an Actor). Each job can run under the same or separate Spark
(sub) Session, and dataframes (as views) can be shared across jobs. Each action (except configuration update) requires one or more input views and produces one 
output view. 

To build the project
```
  mvn clean install -DskipTests
```
[Going through unit tests](docs/unit-tests.md)

### Pipeline definition
The following explains the definition of each section in a pipeline:
- Settings
  - singleSparkSession - when a pipeline consists of multiple jobs, each job can be executed under a separate Spark Sub-Session. This provides the resource isolation 
    across jobs. When this flag is set to true, all jobs are executed under the same global Spark-Session.  
    The default value: false.
  - globalViewAsLocal - global dataframes(views) are shared across jobs (even when they are running under separate Spark Sub-Sessions). Global views are referenced 
    as global_temp.${table}. To make the references easier as local views, set this flag to true.  
    The default value: true.  
  <br />
  
- Variables - the variables defined in this section can be referenced anywhere in the definition of the current pipeline, including sql statements.  
  - A variable must be given a name and value, and is referenced in the format of ${variable-name}.
  - The value can references any values defined in application configuration, as well as from job submit command. The following example shows that process_date 
    is from events.process_date which is defined in application configuration:
      ```xml
      <variables>
          <variable name="process_date" value="${events.process_date}" />
          <variable name="staging_uri" value="/tmp/staging/events" />
      </variables>
      ```
  - **When a variable is defined more than one time, its value from job submit command has the highest precedence, and the value from pipeline definition 
    has the lowest precedence.**
  - When a variable contains sensitive data such as password, its value can be protected by custom key. The following describes the steps how to encrypt 
    the value, and how to configure the variable:
    - encrypt the value by running the following command:
      ```shell
      java -cp spark-etl-framework-xxx.jar com.it21learning.Encryptor --key-string ${key-value} --data ${password}
      ```
      If the key is stored in a file:  
      ```shell
      java -cp spark-etl-framework-xxx.jar com.it21learning.Encryptor --key-file ${file-name} --data ${password}
      ```
      The above command will print the encrypted value.
    - configure the variable:
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

- Aliases - the aliases section defines the short name for referencing various actors. However the alias for each actor must be global unique.
  ```yaml
  aliases:
    - name: file-reader
      type: com.it21learning.etl.source.FileReader
    - name: sql
      type: com.it21learning.etl.transform.SqlTransformer
    - name: hbase-writer
      type: com.it21learning.etl.sink.HBaseWriter
    ```
- Jobs - a pipeline may contain multiple jobs while easy job may have multiple actions. A job provides a container for resource isolation
  (when singleSparkSession = false). The output from an action of a job may be shared across actions within the same job or jobs. Each action
  in a job is represented by an Actor, which is defined by its type, and may have properties for controlling its behavior.
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
  Each Actor has at most one output (as a view). To make the view sharable across jobs, mark the global flag as true. The view can be 
  referenced as table in a sql statement:
  ```sql
  select * from features
  ```
  if the view is global and globalViewAsLocal = true, and global view can be referenced as a local view like in the above query. Otherwise:
  ```sql
  select * from global_temp.features
  ```  

  **The definition of a job is not necessarily embedded in the definition of a pipeline, especially when the job is used across multiple 
  pipelines. Instead the job may be included as follows**
  ```yaml
  jobs:
    - include: jobs/job.yaml
  ```
  where the definition of the job is in a separate file called job.yaml.  
  <br />
 
- Staging - in situation where the results of one or more actions need to be checked for troubleshooting or data verification. This can be 
  achieved by adding the following section in the definition of a pipeline:
  ```yaml
  debug-staging:
    uri: "${staging_uri}"
    actions:
      - transform-events
      - load-events, transform-user-train
  ```
  In above setting, the output of the two actions (load-events, transform-user-train) will be staged at ${staging_uri}.  
  The more actions for staging, the more impact on the performance. Thus normally it happens in dev environments.  
  <br />

Pipeline Examples
- [template_pipeline.yaml](src/test/resources/pipelines/template_pipeline.yaml) with included [job.yaml](src/test/resources/pipelines/jobs/job.yaml) 
- [template_pipeline.json](src/test/resources/pipelines/template_pipeline.json) with included [job.json](src/test/resources/pipelines/jobs/job.json)
- [template_pipeline.xml](src/test/resources/pipelines/template_pipeline.xml) with included [job.xml](src/test/resources/pipelines/jobs/job.xml)

### Configuration
  One custom application configuration can be provided when submitting a Spark job. Normally common variables used across jobs and actions are 
  defined in the application configuration, and they are most environment related. The following is one example:
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
  There are two approaches to provide the runtime configuration:
  - Specify runtime variables when submitting a Spark job. See below the "Submitting a spark-job"
  - Configure runtime in application configuration. See the following example:
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
  **Note: spark configs from application configuration takes highest precedence.**

### Submitting a spark-job
The following is one example of how to submit a spark job. Also it demonstrates how to provide the runtime configs, as well as pass variables.  
```shell
 spark-submit --master yarn|local --deploy-mode client|cluster \
   --name test \
   --conf spark.executor.memory=24g --conf spark.driver.memory=16g \
   --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
   --jars ./mysql-connector-jar.jar \
   --class com.it21learning.etl.Launcher spark-etl-framework-0.1-SNAPSHOT.jar \
   --pipeline-def ./test.yaml --application-conf ./application.conf \
   --var process_date=20200921 --var environment=dev \
   --vars encryption_key=/tmp/app.key,password_key=/tmp/pwd.key \
   --staging-uri hdfs://tmp/staging --staging-actions load-events,combine-users-events \
```
[Run a live example](docs/submit-job.md)

### Source readers
- [FileReader](docs/file-reader.md)
- [FileStreamReader](docs/file-stream-reader.md)
- [FlatReader](docs/flat-reader.md)
- [FlatStreamReader](docs/flat-stream-reader.md)
- [KafkaReader](docs/kafka-reader.md)
- [KafkaStreamReader](docs/kafka-stream-reader.md)
- [DeltaReader](docs/delta-reader.md)
- [DeltaStreamReader](docs/delta-stream-reader.md)
- [JdbcReader](docs/jdbs-reader.md)
- [HBaseReader](docs/hbase-reader.md)
- [RedisReader](docs/redis-reader.md)
- [RedisStreamReader](docs/redis-stream-reader.md)

### Transformers
- [SqlTransformer](docs/sql-transformer.md)

### Sink writers
- [FileWriter](docs/file-writer.md)
- [FileStreamWriter](docs/file-stream-writer.md)
- [KafkaWriter](docs/kafka-writer.md)
- [KafkaStreamWriter](docs/kafka-stream-writer.md)
- [DeltaWriter](docs/delta-writer.md)
- [DeltaStreamWriter](docs/delta-stream-writer.md)
- [JdbcWriter](docs/jdbc-writer.md)
- [JdbcStreamWriter](docs/jdbc-stream-writer.md)
- [HBaseWriter](docs/hbase-writer.md)
- [HBaseStremWriter](docs/hbase-stream-writer.md)
- [RedisWriter](docs/redis-writer.md)
- [RedisStreamWriter](docs/redis-stream-writer.md)

### Writing custom Actor
Indeed in situation where the logic of transforming data is very complicated, or a new reader and/or writer are required, a custom Actor 
can be created by following this [guide](docs/custom-actor.md)

### Spark-SQL practices




