# Laktory TODO List

Planned features, bug fixes and architectural changes

---

## 1. Features

Things that would be nice for the user and enhance usability.

| #  | Description                                                                                                                                          |
|----|------------------------------------------------------------------------------------------------------------------------------------------------------|
| F1 | Add the ability to deploy the terraform state file to Databricks workspace like DAB                                                                  |
| F2 | Add the ability to register Spark Extensions                                                                                                         |
| ~~F3~~ | ~~Add lookup existing resources on all relevant resources~~ ✓                                                                                  |
| ~~F4~~ | ~~Better error message. During validation, it's almost impossible to understand what's going on because multiple models are possible (sources / sinks)~~ ✓ |
| F5 | Let the user run SQL tasks on warehouse instead of job compute                                                                                       |


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

 
## 4. Architecture

Internal improvements


| #  | Description                                                                                                       |
|----|-------------------------------------------------------------------------------------------------------------------|
| A1 | How can I ensure that Claude / GPT knows Laktory and use cases so that users can benefit from it ? MCP?           |
| A2 | How can I offer an AI first solution? Agents that understand lineage and proposes solutions from natural language |

 
## Claude Plan
                                                                                                                                                                                                                                                                                                                             
  ---                                                                                                                                                                                                                                                                                                                        
  Tier 1 — Fix/Ship These First                                                                                                                                                                                                                                                                                              
                                                                                                                                                                                                                                                                                                                             
  F4 — Better validation error messages                                                                                                                                                                                                                                                                                      
  Union-type dispatch (DataSourcesUnion, DataSinksUnion) produces near-useless Pydantic errors when YAML is wrong. This is the single biggest friction point for new users. Self-contained fix with high adoption impact.                                                                                                    
                                                                                                                                                                                                                                                                                                                             
  A1 — MCP server so Claude/GPT understands Laktory                                                                                                                                                                                                                                                                          
  Strategic differentiator. An MCP server exposing model_json_schema() for each major model + pipeline graph introspection means any LLM-enabled tool (Claude Code, Cursor) can generate valid configs and explain pipelines. Scope is manageable and it directly enables A2.                                                
                                                                                                                                                                                                                                                                                                                             
  ---                                        
  Tier 2 — Good Value, Bounded Scope                                                                                                                                                                                                                                                                                         
                                             
  - F3 (lookup existing resources): brownfield adoption — pattern already exists, mostly boilerplate to fill in gaps
  - F5 (SQL tasks on warehouse): cost efficiency for SQL-heavy workloads, scoped to DatabricksJobOrchestrator                                                                                                                                                                                                                
  - P4 (schema_flat.py:73): likely dead code after misplaced return — trivial if confirmed                                                                                                                                                                                                                                   
                                                                                                                                                                                                                                                                                                                             
  ---                                                                                                                                                                                                                                  Strategic differentiator. An MCP server exposing model_json_schema() for each major model + pipeline graph introspection means any LLM-enabled tool (Claude Code, Cursor) can generate valid configs and explain pipelines. Scope

  ---
  Tier 2 — Good Value, Bounded Scope

  - F3 (lookup existing resources): brownfield adoption — pattern already exists, mostly boilerplate to fill in gaps
  - F5 (SQL tasks on warehouse): cost efficiency for SQL-heavy workloads, scoped to DatabricksJobOrchestrator
  - P4 (schema_flat.py:73): likely dead code after misplaced return — trivial if confirmed

  ---
  Tier 3 — Lower Urgency

  - A2 (AI lineage agents): very high ceiling but high effort; fold the DAG-exposure part into A1 as a first step
  - I1 (Claude reviews internal docs): useful housekeeping, low risk
  - F1 (Terraform state in workspace): niche — most users already have remote backends
  - F2 (Spark extensions): low demand
  - P2/P3: worth a look but low blast radius

  ---
  My top recommendation: P1 → F4 → A1, in that order. The MCP server (A1) is the biggest strategic move and would also make Laktory much easier for your own Claude-assisted development.