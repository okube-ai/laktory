"""
Validates the hand-written override pattern on top of a generated base class.

Run with:
    python test_layering.py
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "../.."))

# ---------------------------------------------------------------------------
# Simulate a generated base (normally auto-generated)
# ---------------------------------------------------------------------------

exec(open(Path(__file__).parent / "compare_output/volume_gen_a.py").read())

# ---------------------------------------------------------------------------
# Hand-written override (would live in laktory/models/resources/databricks/)
# ---------------------------------------------------------------------------

from typing import Union
from pydantic import Field, model_validator
from laktory.models.resources.terraformresource import TerraformResource


from laktory.models.basemodel import BaseModel as _BaseModel

class VolumeGrant(_BaseModel):
    """Stub grant for testing purposes."""
    principal: str
    privileges: list[str]


class Volume(VolumeBase):
    """
    Hand-written override that adds Laktory-specific business logic on top
    of the generated VolumeBase.
    """

    # Laktory-specific fields not in Terraform schema
    grant: Union[VolumeGrant, list[VolumeGrant], None] = Field(
        None, description="Grant(s) operating on the Volume"
    )
    grants: list[VolumeGrant] = Field(
        None, description="Grants operating on the Volume"
    )

    @property
    def terraform_excludes(self) -> list[str]:
        return ["grant", "grants"]

    @property
    def full_name(self) -> str:
        parts = [self.catalog_name, self.schema_name, self.name]
        return ".".join(p for p in parts if p)

    @property
    def additional_core_resources(self) -> list:
        # Would generate Grant resources here
        return []


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_isinstance():
    v = Volume(name="landing", catalog_name="dev", schema_name="sources", volume_type="MANAGED")
    assert isinstance(v, TerraformResource), "Volume must be a TerraformResource"
    assert isinstance(v, VolumeBase), "Volume must be a VolumeBase"
    print("[PASS] isinstance checks")


def test_model_dump():
    v = Volume(name="landing", catalog_name="dev", schema_name="sources", volume_type="MANAGED")
    d = v.model_dump(exclude_none=True)
    assert d["name"] == "landing"
    assert d["catalog_name"] == "dev"
    assert "grants" not in d or d.get("grants") is None
    print("[PASS] model_dump")


def test_terraform_properties():
    v = Volume(name="landing", catalog_name="dev", schema_name="sources", volume_type="MANAGED")
    props = v.terraform_properties
    assert props["name"] == "landing"
    assert "grants" not in props, "grants should be excluded from terraform_properties"
    assert "grant" not in props, "grant should be excluded from terraform_properties"
    print("[PASS] terraform_properties excludes Laktory-only fields")


def test_variable_injection():
    v = Volume(
        name="${vars.volume_name}",
        catalog_name="${vars.catalog}",
        schema_name="sources",
        volume_type="MANAGED",
    )
    assert v.name == "${vars.volume_name}", "Variable placeholder should be accepted as-is"
    print("[PASS] variable injection accepted")


def test_full_name():
    v = Volume(name="landing", catalog_name="dev", schema_name="sources", volume_type="MANAGED")
    assert v.full_name == "dev.sources.landing"
    print("[PASS] full_name computed property")


def test_terraform_resource_type():
    v = Volume(name="x", catalog_name="c", schema_name="s", volume_type="MANAGED")
    assert v.terraform_resource_type == "databricks_volume"
    print("[PASS] terraform_resource_type")


if __name__ == "__main__":
    test_isinstance()
    test_model_dump()
    test_terraform_properties()
    test_variable_injection()
    test_full_name()
    test_terraform_resource_type()
    print("\nAll layering tests passed.")
