from laktory.models.resources.providers.baseprovider import BaseProvider
from laktory.models.resources.pulumiresource import PulumiResource
from laktory.models.resources.terraformresource import TerraformResource


class AzureProvider(BaseProvider, PulumiResource, TerraformResource):
    """
    Azure Provider

    Attributes
    ----------
    auxiliary_tenant_ids:
        #TODO
    client_certificate:
        Base64 encoded PKCS#12 certificate bundle to use when authenticating
        as a Service Principal using a Client Certificate
    client_certificate_password:
        The password associated with the Client Certificate. For use when
        authenticating as a Service Principal using a Client Certificate
    client_certificate_path:
        The path to the Client Certificate associated with the Service
        Principal for use when authenticating as a Service Principal using
        a Client Certificate.
    client_id:
        The Client ID which should be used.
    client_id_file_path:
        The path to a file containing the Client ID which should be used.
    client_secret:
        The Client Secret which should be used. For use When authenticating as
        a Service Principal using a Client Secret.
    client_secret_file_path:
        The path to a file containing the Client Secret which should be used.
        For use When authenticating as a Service Principal using a Client
        Secret.
    disable_correlation_request_id:
        This will disable the x-ms-correlation-request-id header.
    disable_terraform_partner_id:
        This will disable the Terraform Partner ID which is used if a custom
        partner_id isn't specified.
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
    oidc_token_file_path:
        The path to a file containing an OIDC ID token for use when
        authenticating as a Service Principal using OpenID Connect.
    partner_id:
        A GUID/UUID that is registered with Microsoft to facilitate partner
        resource usage attribution.
    skip_provider_registration:
        Should the AzureRM Provider skip registering all of the Resource
        Providers that it supports, if they're not already registered? It can
        also be sourced from the following environment variable:
        ARM_SKIP_PROVIDER_REGISTRATION
    storage_use_azuread:
        Should the AzureRM Provider use AzureAD to access the Storage Data
        Plane API's? It can also be sourced from the following environment
        variable: ARM_STORAGE_USE_AZUREAD
    subscription_id:
        The Subscription ID which should be used. It can also be sourced from
        the following environment variable: ARM_SUBSCRIPTION_ID
    tenant_id:
        The Tenant ID which should be used.
    use_aks_workload_identity:
        Allow Azure AKS Workload Identity to be used for Authentication.
    use_cli:
        Allow Azure CLI to be used for Authentication.
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
    client_certificate: str = None
    client_certificate_password: str = None
    client_certificate_path: str = None
    client_id: str = None
    client_id_file_path: str = None
    client_secret: str = None
    client_secret_file_path: str = None
    disable_correlation_request_id: bool = None
    disable_terraform_partner_id: bool = None
    environment: str = None
    # features: ProviderFeaturesArgs = None
    metadata_host: str = None
    msi_endpoint: str = None
    oidc_request_token: str = None
    oidc_request_url: str = None
    oidc_token: str = None
    oidc_token_file_path: str = None
    partner_id: str = None
    skip_provider_registration: bool = None
    storage_use_azuread: bool = None
    subscription_id: str = None
    tenant_id: str = None
    use_aks_workload_identity: bool = None
    use_cli: bool = None
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
        return "pulumi:providers:azure"
