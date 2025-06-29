from pydantic import Field

from laktory.models.resources.providers.baseprovider import BaseProvider
from laktory.models.resources.pulumiresource import PulumiResource


class AzurePulumiProvider(BaseProvider, PulumiResource):
    """
    Azure Pulumi (Native) Provider

    Examples
    --------
    ```py
    from laktory import models

    p = models.AzureProvider(
        client_id="${vars.AZURE_CLIENT_ID}",
        client_secret="${vars.AZURE_CLIENT_SECRET}",
    )
    ```
    """

    auxiliary_tenant_ids: list[str] = Field(None, description="")
    client_certificate_password: str = Field(
        None,
        description="""
    The password associated with the Client Certificate. For use when
    authenticating as a Service Principal using a Client Certificate
    """,
    )
    client_certificate_path: str = Field(
        None,
        description="""
    The path to the Client Certificate associated with the Service Principal for use
    when authenticating as a Service Principal using a Client Certificate.
    """,
    )
    client_id: str = Field(None, description="The Client ID which should be used.")
    client_secret: str = Field(
        None,
        description="""
    The Client Secret which should be used. For use When authenticating as a Service Principal using a Client Secret.
    """,
    )
    disable_pulumi_partner_id: bool = Field(None, description="")
    environment: str = Field(
        None,
        description="""
    The Cloud Environment which should be used. Possible values are public, usgovernment, and china. Defaults to 
    public. It can also be sourced from the following environment variables: AZURE_ENVIRONMENT, ARM_ENVIRONMENT
    """,
    )
    location: str = Field(None, description="")
    metadata_host: str = Field(
        None,
        description="""
    The Hostname which should be used for the Azure Metadata Service. It can also be sourced from the following 
    environment variable: ARM_METADATA_HOSTNAME
    """,
    )
    msi_endpoint: str = Field(
        None,
        description="""
    The path to a custom endpoint for Managed Service Identity - in most circumstances this should be detected 
    automatically.
    """,
    )
    oidc_request_token: str = Field(
        None,
        description="""
    The bearer token for the request to the OIDC provider. For use when authenticating as a Service Principal using 
    OpenID Connect.
    """,
    )
    oidc_request_url: str = Field(
        None,
        description="""
    The URL for the OIDC provider from which to request an ID token. For use when authenticating as a Service Principal 
    using OpenID Connect.
    """,
    )
    oidc_token: str = Field(
        None,
        description="""
    The OIDC ID token for use when authenticating as a Service Principal using OpenID Connect.
    """,
    )
    partner_id: str = Field(
        None,
        description=""""
    A GUID/UUID that is registered with Microsoft to facilitate partner resource usage attribution.
    """,
    )
    subscription_id: str = Field(
        None,
        description="""
    The Subscription ID which should be used. It can also be sourced from the following environment variable: 
    ARM_SUBSCRIPTION_ID
    """,
    )
    tenant_id: str = Field(None, description="The Tenant ID which should be used.")
    use_msi: bool = Field(
        None,
        description="Allow Managed Service Identity to be used for Authentication.",
    )
    use_oidc: bool = Field(
        None, description="Allow OpenID Connect to be used for authentication"
    )

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    # @property
    # def resource_key(self) -> str:
    #     return self.display_name

    # ----------------------------------------------------------------------- #
    # Pulumi Properties                                                       #
    # ----------------------------------------------------------------------- #

    @property
    def pulumi_resource_type(self) -> str:
        return "pulumi:providers:azure_native"
