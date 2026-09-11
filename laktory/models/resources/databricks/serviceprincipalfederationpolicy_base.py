# GENERATED FILE - DO NOT EDIT
# Regenerate with: python scripts/build_resources/01_build.py databricks_service_principal_federation_policy
from __future__ import annotations

from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.terraformresource import TerraformResource


class ServicePrincipalFederationPolicyOidcPolicy(BaseModel):
    audiences: list[str] | None = Field(
        None,
        description="The allowed token audiences, as specified in the 'aud' claim of federated tokens. The audience identifier is intended to represent the recipient of the token. Can be any non-empty string value. As long as the audience in the token matches at least one audience in the policy, the token is considered a match. If audiences is unspecified, defaults to your Databricks account id",
    )
    issuer: str | None = Field(
        None,
        description="The required token issuer, as specified in the 'iss' claim of federated tokens",
    )
    jwks_json: str | None = Field(
        None,
        description="The public keys used to validate the signature of federated tokens, in JWKS format. Most use cases should not need to specify this field. If jwks_uri and jwks_json are both unspecified (recommended), Databricks automatically fetches the public keys from your issuer’s well known endpoint. Databricks strongly recommends relying on your issuer’s well known endpoint for discovering public keys",
    )
    jwks_uri: str | None = Field(
        None,
        description="URL of the public keys used to validate the signature of federated tokens, in JWKS format. Most use cases should not need to specify this field. If jwks_uri and jwks_json are both unspecified (recommended), Databricks automatically fetches the public keys from your issuer’s well known endpoint. Databricks strongly recommends relying on your issuer’s well known endpoint for discovering public keys",
    )
    subject: str | None = Field(
        None,
        description="The required token subject, as specified in the subject claim of federated tokens. Must be specified for service principal federation policies. Must not be specified for account federation policies",
    )
    subject_claim: str | None = Field(
        None,
        description="The claim that contains the subject of the token. If unspecified, the default value is 'sub'",
    )


class ServicePrincipalFederationPolicyBase(BaseModel, TerraformResource):
    """
    Generated base class for `databricks_service_principal_federation_policy`.
    DO NOT EDIT - regenerate from `scripts/build_resources/01_build.py`.
    """

    __doc_generated_base__ = True

    description: str | None = Field(
        None, description="Description of the federation policy"
    )
    policy_id: str | None = Field(None, description="The ID of the federation policy.")
    service_principal_id: int | None = Field(
        None,
        description="The service principal ID that this federation policy applies to.",
    )
    oidc_policy: ServicePrincipalFederationPolicyOidcPolicy | None = Field(None)

    @property
    def terraform_resource_type(self) -> str:
        return "databricks_service_principal_federation_policy"


__all__ = [
    "ServicePrincipalFederationPolicyBase",
    "ServicePrincipalFederationPolicyOidcPolicy",
]
