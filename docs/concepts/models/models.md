Laktory is built on top of a collection of nested [pydantic](https://docs.pydantic.dev/latest/concepts/models/) models. 
Each model is a subclass of `pydantic.BaseModel` and offer a few additional methods and properties. 
The serializable nature of these models makes it possible to define a lakehouse using a declarative approach.

## Declaration
Let's explore the declaration of `Column`, `Table` and `Schema` models as an example. 

### Python Sequential

```py
from laktory import models

x = models.resources.databricks.Column(name="x", type="double")
y = models.resources.databricks.Column(name="y", type="double")
z = models.resources.databricks.Column(name="z", type="double")

table_xy = models.resources.databricks.Table(name="table_xy", columns=[x, y])
table_xyz = models.resources.databricks.Table(name="table_xyz", columns=[x, y, z])

pipeline = models.resources.databricks.Schema(
    catalog_name="dev",
    name="finance",
    tables=[table_xy, table_xyz],
)
```

### Python Nested

```py
from laktory import models

pipeline = models.resources.databricks.Schema(
    catalog_name="dev",
    name="finance",
    tables=[
        {
            "name": "table_xy",
            "columns": [
                {"name": "x", "type": "double"},
                {"name": "y", "type": "double"},
            ],
        },
        {
            "name": "table_xyz",
            "columns": [
                {"name": "x", "type": "double"},
                {"name": "y", "type": "double"},
                {"name": "z", "type": "double"},
            ],
        },
    ],
)
```

### YAML configuration
In most cases however, it is best practice to declare these models as `yaml` configuration files to decouple modeling and implementation.
Here is the same example using a configuration file.

```yaml title="my-schema.yaml"
catalog_name: dev
name: finance
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
    schema = models.resources.databricks.Schema.model_validate_yaml(fp)
```
Using any of the above approaches will result in the exact same `schema` python object.

#### YAML nesting
Laktory supports nested yaml files, meaning that you can include or inject another yaml file from the base
one. Using this approach, the example above could be re-written as

```yaml title="my-schema.yaml"
catalog_name: dev
name: finance
tables: 
  - ${include.table1.yaml}
  - ${include.table2.yaml}
```

```yaml title="table1.yaml"
name: "table_xy"
columns:
  - name: x
    type: double
  - name: y
    type: double
```

```yaml title="table2.yaml"
name: "table_xyz"
columns:
  - name: x
    type: double
  - name: y
    type: double
  - name: z
    type: double
```