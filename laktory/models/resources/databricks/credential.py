from pydantic import Field

from laktory.models.grants.credentialgrant import CredentialGrant
from laktory.models.resources.databricks.credential_base import *  # NOQA: F403 required for documentation
from laktory.models.resources.databricks.credential_base import CredentialBase


class Credential(CredentialBase):
    """
    Databricks Credential

    Unity Catalog service credential, used to access external cloud services.

    Examples
    --------
    ```py
    import io

    from laktory import models

    credential_yaml = '''
    name: azure-service-credential
    purpose: SERVICE
    azure_managed_identity:
      access_connector_id: /subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/rg/providers/Microsoft.Databricks/accessConnectors/ac
    grants:
    - principal: role-engineers
      privileges:
      - ACCESS
    '''
    credential = models.resources.databricks.Credential.model_validate_yaml(
        io.StringIO(credential_yaml)
    )
    ```

    References
    ----------

    * [Databricks Credential](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/credential)
    """

    grant: CredentialGrant | list[CredentialGrant] = Field(
        None,
        description="""
    Non-destructive grant for specific principal(s). Adds or updates privileges for the listed principal(s) and leaves
    grants for all other principals untouched. Use when access is managed from multiple sources (Laktory, Databricks
    UI, etc.). Mutually exclusive with `grants`.
    """,
    )
    grants: list[CredentialGrant] = Field(
        None,
        description="""
    Authoritative grant list for all principals. Replaces every existing grant on this Credential - including those
    set outside Laktory - with only the entries listed here. Use only when Laktory owns all access management for
    this resource. Mutually exclusive with `grant`.
    """,
    )

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    @property
    def additional_core_resources(self) -> list:
        """
        - credential grants
        """
        return self.get_grants_additional_resources(
            object={"credential": f"${{resources.{self.resource_name}.id}}"}
        )

    # ----------------------------------------------------------------------- #
    # Terraform Properties                                                    #
    # ----------------------------------------------------------------------- #

    @property
    def terraform_excludes(self) -> list[str] | dict[str, bool]:
        return ["grant", "grants"]
