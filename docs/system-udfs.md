The following UDFs have been created and ready for use:

- ```com.qwshen.etl.functions.bytes_to_hex(input: ColumnOrName): org.apache.spark.sql.Column```  
  Converts a column containing ```input: Array[Byte]``` to hex-string.
  <br />
  <br />

- ```com.qwshen.etl.functions.bytes_to_string(input: ColumnOrName, charset: String): org.apache.spark.sql.Column```  
  Converts a column containing ```input: Array[Byte]``` to string encoded using ```charset```.
  <br />
  <br />

- ```com.qwshen.etl.functions.com3_to_double(input: ColumnOrName, scale: Int): org.apache.spark.sql.Column```  
  Converts a column containing ```input: Array[Byte]``` to double having a scale of ```scale```.
  <br />
  <br />

- ```com.qwshen.etl.functions.com3_to_int(input: ColumnOrName): org.apache.spark.sql.Column```  
  Converts a column containing ```input: Array[Byte]``` to integer.
  <br />
  <br />

- ```com.qwshen.etl.functions.binary_split(input: Column, delimiter: Array[Byte]): org.apache.spark.sql.Column```  
  Splits a column containing ```input: Array[Byte]``` into array of Array[Byte] by ```delimiter```.