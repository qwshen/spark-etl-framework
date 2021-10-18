- The following UDF is to formalize users' gender into M (for male),  F (for female) and N/A for not available.
```scale
  package com.qwshen.etl.test.udf
  
  object UserUdf {
      val f_gender: UserDefinedFunction = udf {
        (gender: String) => gender match {
          case "M" | "m" | "Male" | "MALE" | "male" => "M"
          case "F" | "f" | "Female" | "FEMALE" | "female" => "F"
          case _ => "N/A"
        }
      }
  }
```
- The following UDF-register is to register the above UDF:
```scaka
  package com.qwshen.etl.test.udf

  class UserUdf extends com.qwshen.etl.common.UdfRegister {
    def register(prefix: String)(implicit session: SparkSession): Unit = {
      session.udf.register(s"${prefix}f_gender", UserUdf.f_gender)
    }
  }
```
- Config udf-registration in a pipeline definition:
```yaml
  udf-registration:
    - prefix: user_
      type: com.qwshen.etl.test.udf.UserUdf

```
- Call the udf in a sql statement
```sql
  select *, user_f_gender(gender) as gender from users
```
