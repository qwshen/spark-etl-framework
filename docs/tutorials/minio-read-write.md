To read from/write to MinIO buckets:
- Set up the configuration for connecting to MinIO cluster with the SparkConfActor
    ```yaml
    actor:
      type: spark-conf-actor
      properties:
        hadoopConfigs:
          fs.s3a.path.style.access: "true"
          fs.s3a.impl: "org.apache.hadoop.fs.s3a.S3AFileSystem"
          fs.s3a.aws.credentials.provider: "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
          fs.s3a.connection.ssl.enabled: "true"
          fs.s3a.connection.timeout: "30000",
          fs.s3a.endpoint: "${minio.host}:${minio.port}"
          fs.s3a.access.key: "${minio.access_key}"
          fs.s3a.secret.key: "${minio.secret_key}"
    ```
    Note: 
    - ${minio.host} - the full-name of a host of the MinIO cluster.
    - ${minio.port} - the port number at the host for connecting to the MinIO cluster
    - ${minio.access_key} & ${minio.secret_key} - the access key & secret key for connecting to the MinIO cluster and accessing objects in buckets.  


- Use FileReader/FileWriter, IcebergReader/IcebergWriter and/or DeltaReader/DeltaWriter for the read/write operations.

  The following is one example of reading csv objects from the users bucket and an underneath input path:
  ```yaml
  actor:
    type: file-reader:
    properties:
      format: csv
      options:
        header: true
        delimiter: "|"
      fileUri: "s3a://users/input"
  ```
