from __future__ import annotations

import pytest
from pydantic import ValidationError

from laktory.models.grants import TableGrant
from laktory.models.resources.databricks import Table
from laktory.models.resources.databricks.grant import Grant
from laktory.models.resources.databricks.grants import Grants


def test_grants():
    # Grant
    table = Table(
        name="slv_stock_prices",
        catalog_name="dev",
        schema_name="markets",
        grant={
            "principal": "users",
            "privileges": [
                "SELECT",
            ],
        },
    )
    assert table.grant == TableGrant(principal="users", privileges=["SELECT"])
    assert len(table.additional_core_resources) == 1
    for r in table.additional_core_resources:
        assert isinstance(r, Grant)
        assert r.resource_name_ == "grant-table-dev-markets-slv_stock_prices-users"

    # Grant x 2
    table = Table(
        name="slv_stock_prices",
        catalog_name="dev",
        schema_name="markets",
        grant=[
            {
                "principal": "users",
                "privileges": [
                    "SELECT",
                ],
            },
            {
                "principal": "admin",
                "privileges": [
                    "MODIFY",
                ],
            },
        ],
    )
    assert table.grant[0] == TableGrant(principal="users", privileges=["SELECT"])
    assert table.grant[1] == TableGrant(principal="admin", privileges=["MODIFY"])
    assert len(table.additional_core_resources) == 2
    for r in table.additional_core_resources:
        assert isinstance(r, Grant)

    # Grants
    table = Table(
        name="slv_stock_prices",
        catalog_name="dev",
        schema_name="markets",
        grants=[
            {
                "principal": "users",
                "privileges": [
                    "SELECT",
                ],
            },
            {
                "principal": "admin",
                "privileges": [
                    "MODIFY",
                ],
            },
        ],
    )
    assert table.grants[0] == TableGrant(principal="users", privileges=["SELECT"])
    assert table.grants[1] == TableGrant(principal="admin", privileges=["MODIFY"])
    assert len(table.additional_core_resources) == 1
    for r in table.additional_core_resources:
        assert isinstance(r, Grants)
        assert r.resource_name_ == "grants-table-dev-markets-slv_stock_prices"

    with pytest.raises(ValidationError):
        Table(
            name="slv_stock_prices",
            catalog_name="dev",
            schema_name="markets",
            grant={
                "principal": "users",
                "privileges": [
                    "SELECT",
                ],
            },
            grants=[
                {
                    "principal": "users",
                    "privileges": [
                        "SELECT",
                    ],
                },
                {
                    "principal": "admin",
                    "privileges": [
                        "MODIFY",
                    ],
                },
            ],
        )


if __name__ == "__main__":
    test_grants()
