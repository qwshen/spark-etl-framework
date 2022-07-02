The VariableSetter can be used to set up variables at job level, and the variables can be referenced in any downstream actors of the job.

- Values of variables in a VariableSetter can reference any variables from application configuration, job-submit arguments and pipeline variables.
- Values of variables can also be any valid sql-expression combining with pre-defined variables.

Actor Class: `com.qwshen.etl.common.VariableSetter`

The definition of the VariableSetter:

- In YAML format
```yaml
  actor:
    type: var-setter
    properties:
      variables:
        properties:
          variables:
            runActor: System
            runDate: "current_date()"
```
- In JSON format
```json
  {
    "actor": {
      "type": "var-writer",
      "properties": {
        "variables": {
          "runActor": "System",
          "runDate": "current_date()"
        }
      }
    }
  }
```
- In XML format
```xml
  <actor type="var-setter">
    <properties>
        <variables>
            <runActor>System</runActor>
            <runTime>now()</runTime>
        </variables>
    </properties>
  </actor>
```

Two variables are defined in above examples:
- runActor
- runDate

In the downstream actors or sql-statements of sql actors, these two variables ${runActor} and ${runDate} can be referenced.