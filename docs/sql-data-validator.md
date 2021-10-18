The SqlDataValidator is for identifying invalid records in a given view, and returns a new view with all valid records.

- Either the validWhere or invalidWhere property needs to be defined in order to identify valid or invalid records.
  The validWhere/invalidWhere is a standard sql statement in where clause.
- The action property must be one of the following values:
  - error: if invalid records found, error out the process
  - staging: all invalid records needs to be staged:
    - staging.uri: location where to stage invalid records
    - staging.format: the file format in which invalid records are staged.
    - staging.options: the options for staging invalid records.
  - ignore: discard all invalid records, and move forward the process with all valid records.
- The view property defines the dataframe to be validated.

The definition of the SqlDataValidator:
- in YAML
```yaml
  actor:
    type: sql-data-validator
    properties:
      validWhere: gender in ('M', 'Male', 'F', 'Female')
      action: staging
      staging:
        uri: /tmp/bad-data/users
        format: parquet
      view: users
```
- in JSON
```json
  {
    "actor": {
      "type": "sql-data-validator",
      "properties": {
        "invalidWhere": "gender not in ('M', 'Male', 'F', 'Female')",
        "actor": "staging",
        "staging": {
          "uri": "/tmp/bad-data/users",
          "format": "csv",
          "options": {
            "header": "true",
            "delimiter": ","
          }
        },
        "view": "users"
      }
    }
  }
```
- in XML
```xml
  <actor type="sql-data-validator">
    <properties>
      <validWhere>gender in ('M', 'Male', 'F', 'Female')</validWhere>
      <action>error</action>
    </properties>
  </actor>
```