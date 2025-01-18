Laktory is built on top of a collection of nested [pydantic](https://docs.pydantic.dev/latest/concepts/models/) models. 
Each model is a subclass of `pydantic.BaseModel` and offer a few additional methods and properties. 
The serializable nature of these models makes it possible to define a lakehouse using a declarative approach.

## Types
Models can generally fall under 2 categories:

- Deployable: Resources defined with the intent of deploying them to cloud or data platform provider. These resources are found under `laktory.models.resources`. 
- Executable: Resources defined with the intent of executing them on a local workstation or on a remote compute resource. A notable example is the `laktory.models.pipeline` model.

## Declaration
Because all models are serializable, they can be declared both directly in python code or as external YAML or JSON files.
The latter is often preferable in the context of a DataOps approach in which the focus should be on the configuration
of the models rather than around the boiler plate code surrounding them.

Let's have a look at how a `Catalog`, `Schema` and `Table` models can be declared using both approaches. 


=== "YAML"
    ```yaml title="catalog.yaml"
    name: prod
    comment: Production catalog
    schemas: 
      - name: finance
        tables:
          - name: bronze
          - name: silver
          - name: gold
      - name: engineering
        tables:
          - name: bronze
          - name: silver
          - name: gold
    ```
    
    ```py title="main.py"
    from laktory import models
    
    with open("catalog.yaml", "r") as fp:
        schema = models.resources.databricks.Catalog.model_validate_yaml(fp)
    ```

=== "Python"
    ```py
    from laktory.models.resources.databricks import Catalog
    from laktory.models.resources.databricks import Schema
    from laktory.models.resources.databricks import Table
    
    bronze = Table(name="bronze")
    silver = Table(name="bronze")
    gold = Table(name="gold")
    
    catalog = Catalog
        name="prod",
        comment="Production catalog",
        schemas=[
            Schema(name="finance", tables=[bronze, silver, gold]),
            Schema(name="engineering", tables=[bronze, silver, gold]),
        ]
    )
    ```

Using any of the above approaches will result in the exact same `catalog` python object.


### YAML nesting
Laktory supports nested yaml files, meaning that you can reference another yaml file 
within a YAML file by using custom tags.

#### Direct injection
With the `!use` tag, one can inject the content of a YAML file directly where its
referenced. Using this approach, the example above could be re-written as

```yaml title="catalog.yaml"
name: "production"
comment: Production catalog
schemas: !use schemas.yaml
```

```yaml title="schemas.yaml"
- name: finance
  tables: !use tables.yaml
- name: engineering
  tables: !use tables.yaml
```

```yaml title="tables.yaml"
- name: bronze
- name: silver
- name: gold
```

#### List Concatenation
In addition to direction injection of an external YAML file, laktory also supports `!extend` tag to concatenate two
lists.

Using this tag, the model
```yaml title="catalog.yaml"
name: production
schemas:
- name: sandbox
- name: bronze
- name: silver
- name: gold
```

could be re-written as:
```yaml title="catalog.yaml"
name: production
schemas:
- name: sandbox
- !extend common_schemas.yaml 
```

```yaml title="common_schemas.yaml"
- name: bronze
- name: silver
- name: gold
```
This is quite convenient when you want to share some components across multiple objects.

#### Dictionary merge
Finally, the `!update` tag is handy to merge the content of two dictionaries.

This combination
```yaml title="catalog.yaml"
name: production
<<: !update catalog_properties.yaml
```

```yaml title="catalog_properties.yaml"
isolation_mode: OPEN
owner: laktory
```

would be the equivalent of
```yaml title="catalog.yaml"
name: production
isolation_mode: OPEN
owner: laktory
```

# Variables
Any Laktory model supports the usage of variables to facilitate parametrization or to
refere a value that might not be available at declaration time.

```yaml title="catalog.yaml"
name: ${vars.env}
comment: Production catalog
schemas: 
  - name: ${vars.env}-finance
  - name: ${vars.env}-engineering
variables:
    env: prod
```

For more information please refer to the [variables](variables.md) documentation.

# Stack
The `laktory.models.Stack` model acts as a container for declaring a collection of
cloud deployable resources. It's the main entry for the Laktory [CLI](cli.md). For
more information, refer to the [documentation](stack.md).