-. (Optional) Mount ADLS storage account in Databricks workspace
   1. Register an Azure AD application
  
      - Search and go to Azure Active Directory.
      - Click on the "App registrations" link, then "New registration".
      - Once an application is registered, get a copy of the Application (client) ID & Directory (tenant) ID.
  
   2. Generate an Authentication Key for the registered application

      - On the registered application page, click on the "Certificates & secrets" link.
      - Click on the "New client secret" link.
      - Pick a lifetime for the secret in the "Expires" drop-down-list, then click on the Add button to add the secret.
      - Get a copy of the Secret from the Value field as the Authentication Key. 
      
   3. Grant the registered application access to a storage account

      - Navigate to the storage account to which the registered application needs to access.
      - Click on the "Access Control (IAM)" link, then "Add role assignment".
      - Select the "Storage Blob Data Contributor" from the list, then click on the Next button
      - Click on the "Select members" link to add the registered application, then click on the "Review + assign" button

   4. Add secrets in Azure Key-Vault

      - Search and go to Azure Key-Vault
      - Click on the Secrets link to add a secret for the Application (client) ID from step 1.
      - Create a secret for the Directory (tenant) ID from step 1.
      - Create a secret for the Authentication Key from step 2.
        
   5. Get the Vault URI & Resource ID

      - On the previous Key-Vault page, click on the Properties link.
      - Get a copy of the "Vault URI" and "Resource ID".
      
   6. Create an Azure Key Vault-backed Secret Scope in Azure Databricks

      - Search and go to Azure Databricks Workspace.
      - Launch a Databricks Workspace.
      - Append secrets/createScope at the end of the workspace url, such as https://???.azuredatabricks.net/?o=???#secrets/createScope.
      - Fill in the "Vault URI" and "Resource ID" from step 6 as the "DNS Name" and "Resource ID" respectively.
      - Click on the Create button

   7. Mount ADLS storage account in Azure Databricks

      - Create the following containers under storage account from step 3.
        - input
        - conf
        - pipelines
        - lib
        - scripts
        - output
      - Create a notebook with scala in the Databricks workspace from step 6.
      - Run the following code in the notebook
        ```scala
        //ws-hands-on-scoep is the name of the secret scope created in step 6
        //adls-client-id is the name of the secret created in step 4 for Application (client) ID 
        val applicationId = dbutils.secrets.get(scope="ws-hands-on-scope",key="adls-client-id")
        //adls-secret-id is the name of the secret created in step 4 for Authentication Key
        val secretId = dbutils.secrets.get(scope="ws-hands-on-scope",key="adls-secret-id")
        //adls-tenant-id is the name of the secret created in step 4 for Directory (tenant) ID
        val tenandId = dbutils.secrets.get(scope="ws-hands-on-scope", key="adls-tenant-id")
        
        val endpoint = "https://login.microsoftonline.com/" + tenandId + "/oauth2/token"
        val configs = Map(
          "fs.azure.account.auth.type" -> "OAuth",
          "fs.azure.account.oauth.provider.type" -> "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id" -> applicationId,
          "fs.azure.account.oauth2.client.secret" -> secretId,
          "fs.azure.account.oauth2.client.endpoint" -> endpoint
        )
        
        //mount the input container under storage account wstorage to /mnt/input
        dbutils.fs.mount("abfss://input@wstorage.dfs.core.windows.net/", "/mnt/input", "", null, configs)
        //mount the conf container under storage account wstorage to /mnt/conf
        dbutils.fs.mount("abfss://conf@wstorage.dfs.core.windows.net/", "/mnt/conf", "", null, configs)
        //mount the lib container under storage account wstorage to /mnt/lib
        dbutils.fs.mount("abfss://lib@wstorage.dfs.core.windows.net/", "/mnt/lib", "", null, configs)
        //mount the scripts container under storage account wstorage to /mnt/scripts
        dbutils.fs.mount("abfss://scripts@wstorage.dfs.core.windows.net/", "/mnt/scripts", "", null, configs)
        //mount the pipelines container under storage account wstorage to /mnt/pipelines
        dbutils.fs.mount("abfss://pipelines@wstorage.dfs.core.windows.net/", "/mnt/pipelines", "", null, configs)
        //mount the output container under storage account wstorage to /mnt/output
        dbutils.fs.mount("abfss://output@wstorage.dfs.core.windows.net/", "/mnt/output", "", null, configs)
        ```
      - List & verify all mounted containers
        ```shell
        %fs
        ls /mnt
        ```

-. Prepare for the Spark job by uploading job data & files.

   - Upload [users.csv](../examples/data/users.csv) to /input/users
   - Upload [train.txt](../examples/data/train.txt) to /input/train
   - Upload [application.conf](azure-databricks/application.conf) to /conf
   - Upload [transform-user-train.sql](../examples/transform-user-train.sql) to /scripts 
   - Upload spark-etl-framework_2.12_3.2.1-1.0.jar to /lib
   - Verify the uploaded files
     ```shell
     %fs
     ls /mnt/input/users
     
     %fs 
     ls /mnt/input/train
     
     %fs 
     ls /mnt/input/conf
     
     %fs 
     ls /mnt/input/pipelines
     
     %fs 
     ls /mnt/input/lib
     
     %fs 
     ls /mnt/input/scripts
     ```

-. Create a Jar job in Databricks workspace  

  In Azure Databricks workspace, create a job with the following properties:

    - Name: fileRead-fileWrite
    - Type: JAR
    - Main class: com.qwshen.etl.Launcher
    - Dependent Library by adding with DBFS/ADLS: dbfs:/mnt/lib/spark-etl-framework_2.12_3.2.1-1.0.jar
    - Cluster: Single Node cluster with Scala 2.12 Spark 3.2.1 and Worker Type Standard_DS3_v2
    - Parameters: ["--pipeline-def","dbfs:/mnt/pipelines/pipeline_fileRead-fileWrite.xml","--application-conf","dbfs:/mnt/conf/application.conf"]
     
-. Run the job & check the result in the output container.