# Laktory TODO

---

## A1 — MCP server ✅

`laktory/AGENTS.md`, `laktory/mcp/` server (4 tools + `get_laktory_docs`), `laktory serve-mcp` CLI, `laktory setup-agent` CLI, `.mcp.json` in all quickstart stacks.

## A2 — AI-first solution

How can we offer an AI-first solution? Agents that understand lineage and propose solutions from natural language.

## A3 — SDP Lakeflow Job dual-mode path

The `SPARK_DECLARATIVE_PIPELINE` orchestrator is designed to serve two execution contexts with identical generated artifacts:

| Mode | Execution | Infrastructure |
|------|-----------|----------------|
| **Local** | `spark-pipelines run --spec …` via subprocess | Local PySpark 4.1+ |
| **Lakeflow Job** | Same script as a Databricks Job task | DBR 16.x; no DLT license required |

**Open question:** how the mode is selected (orchestrator field, deploy-time flag, or inferred from context) is TBD — blocked on testing the Databricks Job execution path.
