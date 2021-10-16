The SchemaValidator is for validating the schema of the input data-frame (view) with the schema provided in the definition.

- The schema can be provided by either ddlSchemaString or ddlSchemaFile property, and must be in DDL format.
- The type of validation must be either match or adapt
  - match: the two schemas must be matched by number of columns, the name & data type of each column
  - adapt: the schema of the dataframe is adapted to the schema from the definition. As a result of the validation, the output dataframe will be "expanded".  
    **Note: If the schema from the definition has columns that don't exist in the schema of the dataframe, new columns will be added with null values.**
- The mode of validation must be either strict or default
  - when the validation type is match
    - strict: the order of the columns in two schemas must be the same
    - default: ignore the order of the columns from both schemas.
  - when the validation type is adapt
    - strict: all columns from the dataframe must be included in the schema from the definition.
    - default: ignore columns from the dataframe that don't exist in the schema from the definition  
      **Note: this will cause data lost**
- The action after the validation must be either error or ignore
  - error: if the validation fails, the process exits with the validation error.
  - ignore: the process ignores the validation failure, but logs will written.
- The view is the input dataframe that its schema is validated with the schema provided in the definition.

The definition of the SchemaValidator:
- in YAML
```yaml
  actor:
    type: schema-validator
    properties:
      ddlSchemaString: "id int, name string, age int, gender string, address string"
      type: match
      mode: strict
      action: error
      view: users
```
- in JSON
```json
  {
    "actor": {
      "type": "schema-validator",
      "properties": {
        "ddlSchemaFile": "${application.users.schema",
        "type": "adapt",
        "mode": "default",
        "action": "ignore",
        "view": "users"
      }
    }
  }
```
- in XML
```xml
  <actor type="schema-validator">
    <properties>
      <ddlSchemString>id int, name string, age int, gender string, address string</ddlSchemString>
      <type>adapt</type>
      <mode>strict</mode>
      <action>ignore</action>
      <view>users</view>
    </properties>
  </actor>
```