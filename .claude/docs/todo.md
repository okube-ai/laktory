# Laktory TODO List

Planned features, bug fixes and architectural changes

---

## 1. Features

Things that would be nice for the user and enhance usability.

| #  | Description                                                                         |
|----|-------------------------------------------------------------------------------------|
| F1 | Add the ability to deploy the terraform state file to Databricks workspace like DAB |
| F2 | Add the ability to register Spark Extensions                                        |


---

## 2. Bug Fixes

Issues that need to be resolved

| # | Description                                          |
|---|------------------------------------------------------|
| P1 | `laktory/models/basemodel.py:445,450,555`            |
| P2 | `laktory/models/basemodel.py:459`                    |
| P3 | `laktory/yaml/recursiveloader.py:149`                |
| P4 | `laktory/narwhals_ext/dataframe/schema_flat.py:73`   |

## 3. Internal

Internal improvements


| #  | Description                                                                                                      |
|----|------------------------------------------------------------------------------------------------------------------|
| I1 | Ask Claude to review its internal documentation, restructure and optimize. What are we missing?                  |
| I2 | Always authorize Claud to Newline followed by # inside a quoted argument can hide arguments from path validation |
| I3 | Add instructions for Claude to have access to secrets for running unit tests?                                    |

 