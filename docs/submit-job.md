- Create a work directory and make it as the current directory, such as
```shell
mkdir ~/workspace
cd ~/workspace
```

- Create the application configuration as [this](examples/application.conf)
- Prepare test data
  ```shell
  mkdir data/users
  mkdir data/train
  ```
  - Create a users.csv file in data/users with [this](examples/data/users.csv)
  - Create a train.txt file in data/train with [this](examples/data/train.txt)
- Create sql statement for transforming users & train
  ```shell
  mkdir scripts
  ```
  Create a transform-user-train.sql file as [this](examples/transform-user-train.sql)  
  <br />

- Create the pipeline as [this](examples/pipeline_fileRead-fileWrite.xml)  
  <br />

- Compile the project & copy the jar file (spark-etl-framework-xxx.jar) to the current directory.  
  <br />

- Submit the job
  ```shell
  spark-submit --master local --deploy-mode client \
  --name user-train --conf spark.executor.memory=8g --conf spark.driver.memory=4g \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --class com.qwshen.Launcher spark-etl-framework-xxx.jar \
  --pipeline-def ./pipeline_fileRead-fileWrite.xml --application-conf ./application.conf \
  --var application.process_date=20200921
  ```
- Check & review the result in data/features  
