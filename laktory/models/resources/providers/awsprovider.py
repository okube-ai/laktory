from pydantic import Field

from laktory.models.basemodel import BaseModel
from laktory.models.resources.providers.baseprovider import BaseProvider
from laktory.models.resources.pulumiresource import PulumiResource
from laktory.models.resources.terraformresource import TerraformResource


class ProviderDefaultTags(BaseModel):
    tags: dict[str, str] = None


class ProviderIgnoreTagsArgs(BaseModel):
    key_prefixes: list[str] = None
    keys: list[str] = None


class ProviderAssumeRole(BaseModel):
    duration: str = None
    external_id: str = None
    policy: str = None
    policy_arns: list[str] = None
    role_arn: str = None
    session_name: str = None
    source_identity: str = None
    tags: dict[str, str] = None
    transitive_tag_keys: list[str] = None


class ProviderAssumeRoleWithWebIdentity(BaseModel):
    duration: str = None
    policy: str = None
    policy_arns: list[str] = None
    role_arn: str = None
    session_name: str = None
    web_identity_token: str = None
    web_identity_token_file: str = None


class AWSProvider(BaseProvider, PulumiResource, TerraformResource):
    """
    AWS Provider

    Examples
    --------
    ```py
    from laktory import models

    p = models.AWSProvider(
        access_key="${vars.AWS_ACCESS_KEY}",
    )
    ```
    """

    access_key: str = Field(
        None,
        description="""
        The access key for API operations. You can retrieve this from the 'Security & Credentials' section of the AWS
        console.
        """,
    )
    allowed_account_ids: list[str] = None
    assume_role: ProviderAssumeRole = None
    assume_role_with_web_identity: ProviderAssumeRoleWithWebIdentity = None
    custom_ca_bundle: str = Field(
        None,
        description="""File containing custom root and intermediate certificates. Can also be configured using the 
        AWS_CA_BUNDLE environment variable. (Setting ca_bundle in the shared config file is not supported.)
        """,
    )
    default_tags: ProviderDefaultTags = Field(
        None,
        description="Configuration block with settings to default resource tags across all resources.",
    )
    ec2_metadata_service_endpoint: str = Field(
        None,
        description="""
        Address of the EC2 metadata service endpoint to use. Can also be configured using the 
        AWS_EC2_METADATA_SERVICE_ENDPOINT environment variable.
        """,
    )
    ec2_metadata_service_endpoint_mode: str = Field(
        None,
        description="""
        Protocol to use with EC2 metadata service endpoint.Valid values are IPv4 and IPv6. Can also be configured 
        using the AWS_EC2_METADATA_SERVICE_ENDPOINT_MODE environment variable.
        """,
    )
    # endpoints: SequenceProviderEndpointArgs]] = None
    forbidden_account_ids: list[str] = Field(None, description="")
    http_proxy: str = Field(
        None,
        description="""
        URL of a proxy to use for HTTP requests when accessing the AWS API. Can also be set using the HTTP_PROXY 
        or http_proxy environment variables.
        """,
    )
    https_proxy: str = Field(
        None,
        description="""
        URL of a proxy to use for HTTPS requests when accessing the AWS API. Can also be set using the HTTPS_PROXY
        or https_proxy environment variables.
        """,
    )
    ignore_tags: ProviderIgnoreTagsArgs = Field(
        None,
        description="Configuration block with settings to ignore resource tags across all resources.",
    )
    insecure: bool = Field(
        None,
        description="Explicitly allow the provider to perform 'insecure' SSL requests. If omitted, default value is false",
    )
    max_retries: int = Field(
        None,
        description="""
        The maximum number of times an AWS API request is being executed. If the API request still fails, an error 
        is thrown.
        """,
    )
    no_proxy: str = Field(
        None,
        description="""
        Comma-separated list of hosts that should not use HTTP or HTTPS proxies. Can also be set using the NO_PROXY
        or no_proxy environment variables.
        """,
    )
    profile: str = Field(
        None,
        description="The profile for API operations. If not set, the default profile created with aws configure will be used.",
    )
    region: str = Field(
        None,
        description="""
        The region where AWS operations will take place. Examples are us-east-1, us-west-2, etc. It can also be
        sourced from the following environment variables: AWS_REGION, AWS_DEFAULT_REGION
        """,
    )
    retry_mode: str = Field(
        None,
        description="""
        Specifies how retries are attempted. Valid values are standard and adaptive. Can also be configured using 
        the AWS_RETRY_MODE environment variable.
        """,
    )
    s3_us_east1_regional_endpoint: str = Field(
        None,
        description="""
        Specifies whether S3 API calls in the us-east-1 region use the legacy global endpoint or a regional endpoint.
        Valid values are legacy or regional. Can also be configured using the AWS_S3_US_EAST_1_REGIONAL_ENDPOINT 
        environment variable or the s3_us_east_1_regional_endpoint shared config file parameter
        """,
    )
    s3_use_path_style: bool = Field(
        None,
        description="""
        Set this to true to enable the request to use path-style addressing, i.e., https://s3.amazonaws.com/BUCKET/KEY.
        By default, the S3 client will use virtual hosted bucket addressing when possible 
        (https://BUCKET.s3.amazonaws.com/KEY). Specific to the Amazon S3 service.
        """,
    )
    secret_key: str = Field(
        None,
        description="""
        The secret key for API operations. You can retrieve this from the 'Security & Credentials' section of the AWS 
        console.
        """,
    )
    shared_config_files: list[str] = Field(
        None,
        description="List of paths to shared config files. If not set, defaults to [~/.aws/config].",
    )
    shared_credentials_files: list[str] = Field(
        None,
        description="List of paths to shared credentials files. If not set, defaults to [~/.aws/credentials].",
    )
    skip_credentials_validation: bool = Field(
        None,
        description="""
        Skip the credentials validation via STS API. Used for AWS API implementations that do not have STS 
        available/implemented.
        """,
    )
    skip_metadata_api_check: bool = Field(
        None,
        description="""
        Skip the AWS Metadata API check. Used for AWS API implementations that do not have a metadata api endpoint.
        """,
    )
    skip_region_validation: bool = Field(
        None,
        description="""
        Skip static validation of region name. Used by users of alternative AWS-like APIs or users w/ access to
        regions that are not public (yet).
        """,
    )
    skip_requesting_account_id: bool = Field(
        None,
        description="""
        Skip requesting the account ID. Used for AWS API implementations that do not have IAM/STS
        API and/or metadata API.
        """,
    )
    sts_region: str = Field(
        None,
        description="The region where AWS STS operations will take place. Examples are us-east-1 and us-west-2.",
    )
    token: str = Field(
        None,
        description="Session token. A session token is only required if you are using temporary security credentials.",
    )
    use_dualstack_endpoint: bool = Field(
        None, description="Resolve an endpoint with DualStack capability"
    )
    use_fips_endpoint: bool = Field(
        None, description="Resolve an endpoint with FIPS capability"
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
        return "pulumi:providers:aws"
