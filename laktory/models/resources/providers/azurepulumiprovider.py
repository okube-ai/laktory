from laktory.models.resources.providers.baseprovider import BaseProvider
from laktory.models.resources.pulumiresource import PulumiResource


class AzurePulumiProvider(BaseProvider, PulumiResource):
    """
    Azure Pulumi (Native) Provider

    Attributes
    ----------
    auxiliary_tenant_ids:
        #TODO
    client_certificate_password:
        The password associated with the Client Certificate. For use when
        authenticating as a Service Principal using a Client Certificate
    client_certificate_path:
        The path to the Client Certificate associated with the Service
        Principal for use when authenticating as a Service Principal using
        a Client Certificate.
    client_id:
        The Client ID which should be used.
    client_secret:
        The Client Secret which should be used. For use When authenticating as
        a Service Principal using a Client Secret.
    environment:
        The Cloud Environment which should be used. Possible values are public,
        usgovernment, and china. Defaults to public. It can also be sourced
        from the following environment variables: AZURE_ENVIRONMENT,
        ARM_ENVIRONMENT
    metadata_host:
        The Hostname which should be used for the Azure Metadata Service. It
        can also be sourced from the following environment variable:
        ARM_METADATA_HOSTNAME
    msi_endpoint:
        The path to a custom endpoint for Managed Service Identity - in most
        circumstances this should be detected automatically.
    oidc_request_token:
        The bearer token for the request to the OIDC provider. For use
        when authenticating as a Service Principal using OpenID Connect.
    oidc_request_url:
        The URL for the OIDC provider from which to request an ID token. For
        use when authenticating as a Service Principal using OpenID Connect.
    oidc_token:
        The OIDC ID token for use when authenticating as a Service Principal
        using OpenID Connect.
    partner_id:
        A GUID/UUID that is registered with Microsoft to facilitate partner
        resource usage attribution.
    subscription_id:
        The Subscription ID which should be used. It can also be sourced from
        the following environment variable: ARM_SUBSCRIPTION_ID
    tenant_id:
        The Tenant ID which should be used.
    use_msi:
        Allow Managed Service Identity to be used for Authentication.
    use_oidc:
        Allow OpenID Connect to be used for authentication


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

    auxiliary_tenant_ids: list[str] = None
    client_certificate_password: str = None
    client_certificate_path: str = None
    client_id: str = None
    client_secret: str = None
    disable_pulumi_partner_id: bool = None
    environment: str = None
    location: str = None
    metadata_host: str = None
    msi_endpoint: str = None
    oidc_request_token: str = None
    oidc_request_url: str = None
    oidc_token: str = None
    partner_id: str = None
    subscription_id: str = None
    tenant_id: str = None
    use_msi: bool = None
    use_oidc: bool = None

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
