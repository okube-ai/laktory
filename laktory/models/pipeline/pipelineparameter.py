from pydantic import Field

from laktory.models.basemodel import BaseModel


class PipelineParameter(BaseModel):
    """
    Pipeline parameter, referenced as `${params.<name>}` in the pipeline nodes and
    resolved at run time, e.g. from the job parameters of a Lakeflow Job orchestrator.
    Used to reuse a single deployed pipeline across clients, tenants, regions, etc.

    Examples
    --------
    ```py
    import laktory as lk

    pl = lk.models.Pipeline(
        name="pl-sales",
        parameters={"client": {}, "region": {"default": "eu"}},
        nodes=[
            {
                "name": "slv_sales",
                "source": {"table_name": "brz_sales"},
                "sinks": [
                    {
                        "schema_name": "${params.client}_silver",
                        "table_name": "slv_sales",
                    }
                ],
            }
        ],
    )

    pl_acme = pl.resolve_parameters({"client": "acme"})
    print(pl_acme.nodes[0].sinks[0].schema_name)
    # > acme_silver
    ```

    References
    ----------
    * [Variables](https://www.laktory.ai/concepts/variables/)
    """

    default: str | None = Field(
        None,
        description="Default value. Without a default, the parameter must be set for each run.",
    )
