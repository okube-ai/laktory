# Laktory TODO List

Planned features, bug fixes and architectural changes

---

## 1. Features

Things that would be nice for the user and enhance usability.

| #      | Description                                                                                                                                                |
|--------|------------------------------------------------------------------------------------------------------------------------------------------------------------|
| F1     | Add the ability to deploy the terraform state file to Databricks workspace like DAB                                                                        |
| ~~F2~~ | ~~Add the ability to register Spark Extensions~~ ✓                                                                                                        |
| ~~F3~~ | ~~Add lookup existing resources on all relevant resources~~ ✓                                                                                              |
| ~~F4~~ | ~~Better error message. During validation, it's almost impossible to understand what's going on because multiple models are possible (sources / sinks)~~ ✓ |
| ~~F5~~ | ~~Let the user run SQL tasks on warehouse instead of job compute~~ Too complex (metadata, full refresh, local execution) for the added benefits.           |
| F6     | Integration of Spark-native declarative piplines                                                                                                           |


---

## 2. Bug Fixes

Issues that need to be resolved

| # | Description                                          |
|---|------------------------------------------------------|

## 3. Internal

Internal improvements


| #  | Description                                                                                                      |
|----|------------------------------------------------------------------------------------------------------------------|
| I1 | Ask Claude to review its internal documentation, restructure and optimize. What are we missing?                  |
| I2 | Always authorize Claud to Newline followed by # inside a quoted argument can hide arguments from path validation |
| I3 | Add instructions for Claude to have access to secrets for running unit tests?                                    |

 
## 4. Architecture

Internal improvements


| #  | Description                                                                                                       |
|----|-------------------------------------------------------------------------------------------------------------------|
| A1 | How can I ensure that Claude / GPT knows Laktory and use cases so that users can benefit from it ? MCP?           |
| A2 | How can I offer an AI first solution? Agents that understand lineage and proposes solutions from natural language |
