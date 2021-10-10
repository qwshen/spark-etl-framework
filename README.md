Spark-etl-framework is a pipeline based data transformation framework using Spark-SQL. For one process to transform & move data from end to end, a pipeline needs to be defined,
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
  
- Variables - the variables defined in this section can be referenced any where in the definition of the current pipeline, including sql statements.  
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
    
- Aliases
- Jobs
- Staging

Pipeline Examples

### Configuration

### Submitting a spark-job
```shell
 spark-submit --master yarn|local --deploy-mode client|cluster \
   --name test \
   --conf spark.executor.memory=24g --conf spark.driver.memory=16g \
   --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
   --jars ./mysql-connector-jar.jar,./mongo-jara-driver-3.9.1.jar \
   --class com.it21learning.etl.Launcher spark-etl-framework-0.1-SNAPSHOT.jar \
   --pipeline-def ./test.yaml --application-conf ./application.conf \
   --var process_date=20200921 --var environment=dev \
   --vars encryption_key=/tmp/app.key,password_key=/tmp/pwd.key \
   --staging-uri hdfs://tmp/staging --staging-actions load-events,combine-users-events \
```

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

### Spark-SQL practices




spark-submit --master local --deploy-mode client \
--name user-train --conf spark.executor.memory=8g --conf spark.driver.memory=4g \
--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
--class com.it21learning.etl.Launcher spark-etl-framework-1.0-SNAPSHOT.jar \
--pipeline-def ./pipeline_fileRead-fileWrite.xml --application-conf ./application.conf \
--var application.process_date=20200921 
