??? "API Documentation"
    [`laktory.models`](TODO)<br>

Laktory is built on top of a collection of nested [pydantic](https://docs.pydantic.dev/latest/concepts/models/) models. 
Each model is a subclass of `pydantic.BaseModel` and offer a few additional methods and properties. 
The serializable nature of these models makes it possible to define a lakehouse using a declarative approach.

## Declaration
Let's explore the declaration of `Column`, `Table` and `Pipeline` models as an example. 

### Python Sequential
```py
from laktory import models

x = models.Column(name="x", type="double")
y = models.Column(name="y", type="double")
z = models.Column(name="z", type="double")

table_xy = models.Table(name="table_xy", columns=[x,y])
table_xyz = models.Table(name="table_xyz", columns=[x,y, z])

pipeline = models.Pipeline(
    name="my-pipline",
    catalog="dev",
    target="finance",
    tables=[table_xy, table_xyz],
)
```

### Python Nested
```py
from laktory import models

pipeline = models.Pipeline(
    name="my-pipline",
    catalog="dev",
    target="finance",
    tables=[
        {
            "name": "table_xy", "columns": [
                {"name": "x", "type": "double"},
                {"name": "y", "type": "double"},
            ]
         },
        {
            "name": "table_xyz", "columns": [
                {"name": "x", "type": "double"},
                {"name": "y", "type": "double"},
                {"name": "z", "type": "double"},
            ]
         },
    ]
)
```

### YAML configuration
In most cases however, it is best practice to declare these models as `yaml` configuration files to decouple modeling and implementation.
Here is the same example using a configuration file.

```yaml title="my-pipeline.yaml"
name: my-pipline
catalog: dev
target: finance
tables: 
  - name: "table_xy"
    columns:
      - name: x
        type: double
      - name: y
        type: double
  - name: "table_xyz"
    columns:
      - name: x
        type: double
      - name: y
        type: double
      - name: z
        type: double    
```

```py title="main.py"
from laktory import models
with open("my-pipeline.yaml", "r") as fp:
  pipeline = models.Pipeline.model_validate_yaml(fp)
```
Using any of the above approaches will result in the exact same `pipeline` python object.

## Variables
In some cases, it's not practical or even possible to declare a property as plain text.
Take for example the declaration of a pipeline where the catalog name is the environment in which the pipeline will be deployed.

```yaml
name: my-pipline
catalog: dev
target: finance
tables: 
  - name: "table"
    columns:
      - name: x
        type: double
...
```
You probably want to re-use the same configuration file for all your environments, but with a different value for the catalog. 
Laktory makes it possible by introducing the concept of models variables or `vars`, declared as `${var.variable_name}`

```yaml
name: my-pipline
catalog: ${var.env}
target: finance
tables: 
  - name: "table"
    columns:
      - name: x
        type: double
...
```

Once the object has been instantiated, it can be assigned variable values.  

```py title="main.py"
import os
from laktory import models

with open("my-pipeline.yaml", "r") as fp:
    pipeline = models.Pipeline.model_validate_yaml(fp)

pipeline.vars = {"env": os.getenv("ENV")}
```

These variables won't affect serialization (`model_dump()`) until the method `inject_vars()` is called. 
This is especially useful when your model needs to reference id of a resource that has not yet been deployed at runtime.
