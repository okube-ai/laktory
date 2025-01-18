# Laktory Documentation

Laktory is built on top of a collection of nested [pydantic](https://docs.pydantic.dev/latest/concepts/models/) models. 
Each model is a subclass of `pydantic.BaseModel` and offers a few additional methods and properties. 
The serializable nature of these models makes it possible to define a lakehouse using a declarative approach.

## Types
Models generally fall into two categories:

- **Deployable**: Resources defined with the intent of deploying them to a cloud or data platform provider. These resources are found under `laktory.models.resources`.
- **Executable**: Resources defined with the intent of executing them on a local workstation or a remote compute resource. A notable example is the `laktory.models.pipeline` model.

## Declaration
Because all models are serializable, they can be declared either directly in Python code or as external YAML or JSON files.
The latter is often preferable in a DataOps approach, where the focus is on configuring models rather than writing boilerplate code.

Below is an example of how `Catalog`, `Schema`, and `Table` models can be declared using both approaches.


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

Both approaches result in the exact same `catalog` Python object.


### YAML nesting
Laktory supports nested YAML files, allowing you to reference another YAML file within a YAML file using custom tags.

#### Direct injection
With the `!use` tag, you can inject the content of a YAML file directly where it is referenced. The example above can be
rewritten as:

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
In addition to direct injection, Laktory also supports the `!extend` tag to concatenate two lists. For example:

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
This is convenient for sharing components across multiple objects.

#### Dictionary merge
The `!update` tag allows merging the content of two dictionaries. For instance:

```yaml title="catalog.yaml"
name: production
<<: !update catalog_properties.yaml
```

```yaml title="catalog_properties.yaml"
isolation_mode: OPEN
owner: laktory
```

Is equivalent to:
```yaml title="catalog.yaml"
name: production
isolation_mode: OPEN
owner: laktory
```

## Variables
Laktory models support variables to facilitate parameterization or to reference values unavailable at declaration time.

```yaml title="catalog.yaml"
name: ${vars.env}
comment: Production catalog
schemas: 
  - name: ${vars.env}-finance
  - name: ${vars.env}-engineering
variables:
    env: prod
```

For more information, refer to the [variables documentation](variables.md).

## Stack
The `laktory.models.Stack` model acts as a container for declaring a collection of cloud-deployable resources. It serves as the main entry point for the Laktory [CLI](cli.md). For
more information, refer to the [stack documentation](stack.md).