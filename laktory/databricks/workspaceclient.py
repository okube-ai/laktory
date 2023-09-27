import time
from databricks.sdk import WorkspaceClient as _WorkspaceClient
from databricks.sdk.service.sql import StatementState

from laktory import settings


class WorkspaceClient(_WorkspaceClient):
    def __init__(self, *args, **kwargs):
        kwargs["host"] = kwargs.get("host", settings.databricks_host)
        kwargs["token"] = kwargs.get("token", settings.databricks_token)
        super().__init__(*args, **kwargs)

    def execute_statement_and_wait(
        self, statement, warehouse_id=None, catalog_name=None
    ):
        # Settings
        if warehouse_id is None:
            warehouse_id = settings.databricks_warehouse_id

        r = self.statement_execution.execute_statement(
            statement=statement,
            catalog=catalog_name,
            warehouse_id=warehouse_id,
        )
        statement_id = r.statement_id
        state = r.status.state

        while state in [StatementState.PENDING, StatementState.RUNNING]:
            r = self.statement_execution.get_statement(statement_id)
            time.sleep(1)
            state = r.status.state

        if state != StatementState.SUCCEEDED:
            # TODO: Create specific error
            print(r.status.error.message)
            raise Exception(r.status.error)

        return r
