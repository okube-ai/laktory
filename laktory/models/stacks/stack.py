import json
import os
import re
import time
from typing import Any
from typing import Literal

from pydantic import Field
from pydantic import field_validator
from pydantic import model_validator

from laktory._logger import get_logger
from laktory._settings import settings
from laktory.models.basemodel import BaseModel
from laktory.models.pipeline.pipeline import Pipeline
from laktory.models.resources.databricks.accesscontrolruleset import (
    AccessControlRuleSet,
)
from laktory.models.resources.databricks.alert import Alert
from laktory.models.resources.databricks.app import App
from laktory.models.resources.databricks.catalog import Catalog
from laktory.models.resources.databricks.cluster import Cluster
from laktory.models.resources.databricks.clusterpolicy import ClusterPolicy
from laktory.models.resources.databricks.connection import Connection
from laktory.models.resources.databricks.currentuser import CurrentUser
from laktory.models.resources.databricks.dashboard import Dashboard
from laktory.models.resources.databricks.dbfsfile import DbfsFile
from laktory.models.resources.databricks.directory import Directory
from laktory.models.resources.databricks.entitlements import Entitlements
from laktory.models.resources.databricks.externallocation import ExternalLocation
from laktory.models.resources.databricks.grant import Grant
from laktory.models.resources.databricks.grants import Grants
from laktory.models.resources.databricks.group import Group
from laktory.models.resources.databricks.job import Job
from laktory.models.resources.databricks.metastore import Metastore
from laktory.models.resources.databricks.metastoreassignment import MetastoreAssignment
from laktory.models.resources.databricks.metastoredataaccess import MetastoreDataAccess
from laktory.models.resources.databricks.mlflowexperiment import MLflowExperiment
from laktory.models.resources.databricks.mlflowmodel import MLflowModel
from laktory.models.resources.databricks.mlflowwebhook import MLflowWebhook
from laktory.models.resources.databricks.mwsnetworkconnectivityconfig import (
    MwsNetworkConnectivityConfig,
)
from laktory.models.resources.databricks.notebook import Notebook
from laktory.models.resources.databricks.notificationdestination import (
    NotificationDestination,
)
from laktory.models.resources.databricks.obotoken import OboToken
from laktory.models.resources.databricks.permissions import Permissions
from laktory.models.resources.databricks.pipeline import Pipeline as DatabricksPipeline
from laktory.models.resources.databricks.pythonpackage import PythonPackage
from laktory.models.resources.databricks.qualitymonitor import QualityMonitor
from laktory.models.resources.databricks.query import Query
from laktory.models.resources.databricks.recipient import Recipient
from laktory.models.resources.databricks.repo import Repo
from laktory.models.resources.databricks.schema import Schema
from laktory.models.resources.databricks.secret import Secret
from laktory.models.resources.databricks.secretscope import SecretScope
from laktory.models.resources.databricks.serviceprincipal import ServicePrincipal
from laktory.models.resources.databricks.share import Share
from laktory.models.resources.databricks.storagecredential import StorageCredential
from laktory.models.resources.databricks.table import Table
from laktory.models.resources.databricks.user import User
from laktory.models.resources.databricks.vectorsearchendpoint import (
    VectorSearchEndpoint,
)
from laktory.models.resources.databricks.vectorsearchindex import VectorSearchIndex
from laktory.models.resources.databricks.volume import Volume
from laktory.models.resources.databricks.warehouse import Warehouse
from laktory.models.resources.databricks.workspacebinding import WorkspaceBinding
from laktory.models.resources.databricks.workspacefile import WorkspaceFile
from laktory.models.resources.databricks.workspacetree import WorkspaceTree
from laktory.models.resources.providers.awsprovider import AWSProvider
from laktory.models.resources.providers.azureprovider import AzureProvider
from laktory.models.resources.providers.baseprovider import BaseProvider
from laktory.models.resources.providers.databricksprovider import DatabricksProvider

logger = get_logger(__name__)

DIRPATH = "./"

_WS_TOKEN_CACHE = ".laktory.ws-backend-token.json"


def _get_cached_ws_token(wc, cache_path: str) -> str:
    """Return a Databricks PAT suitable for HTTP Basic auth, caching it on disk.

    Creates a new PAT only when no cached token exists or the cached token
    expires within 24 hours. The cache file lives alongside stack.tf.json so
    it is scoped to the build directory and stays out of source control.
    """
    if os.path.exists(cache_path):
        try:
            with open(cache_path) as f:
                cached = json.load(f)
            expires_at = cached.get("expires_at")
            if expires_at is None or expires_at > time.time() + 86400:
                return cached["token"]
            # Token expires within 24 hours — delete and rotate
            try:
                wc.tokens.delete(token_id=cached["token_id"])
            except Exception:
                pass
        except Exception:
            pass

    result = wc.tokens.create(comment="laktory-tfstate (auto-generated)")
    expiry_ms = result.token_info.expiry_time
    cached = {
        "token": result.token_value,
        "token_id": result.token_info.token_id,
        "expires_at": expiry_ms / 1000 if expiry_ms else None,
    }
    os.makedirs(os.path.dirname(cache_path) or ".", exist_ok=True)
    with open(cache_path, "w") as f:
        json.dump(cached, f, indent=2)

    return result.token_value


def _resolve_db_token(provider) -> str:
    """Return a concrete Databricks Bearer token for any supported auth method."""
    if provider.token:
        return provider.token

    try:
        headers = provider.workspace_client.config.authenticate()
        auth = headers.get("Authorization", "")
        return auth.removeprefix("Bearer ").removeprefix("Basic ")
    except Exception as exc:
        raise ValueError(
            "backend.databricks_workspace: could not resolve a Databricks token from "
            "the provider credentials. Ensure at least one auth method is configured "
            "(token, client_id/secret, azure_client_id/secret, profile). "
            f"Original error: {exc}"
        ) from exc


class Terraform(BaseModel):
    backend: dict[str, Any] | None = Field(
        None,
        description=(
            "Terraform backend configuration. Accepts any standard Terraform backend "
            "block (e.g. `azurerm`, `s3`, `http`). Additionally, the special key "
            "`databricks_workspace: true` auto-configures a Terraform HTTP backend "
            "that stores state as a workspace file in the current Databricks user's "
            "directory at "
            "`/Users/{user}/.laktory/{stack}/{env}/state/terraform.tfstate`. "
            "No external storage account is required."
        ),
    )


class LaktorySettings(BaseModel):
    """
    Laktory Settings
    """

    dataframe_backend: str = Field(None, description="DataFrame backend")
    dataframe_api: Literal["NARWHALS", "NATIVE"] = Field(None, description="")
    workspace_root: str = Field(
        "/.laktory/",
        description="Root directory of a Databricks Workspace (excluding `'/Workspace') to which databricks objects like notebooks and workspace files are deployed.",
    )
    runtime_root: str = Field(
        "/laktory/",
        description="Laktory cache root directory. Used when a pipeline needs to write checkpoint files.",
    )
    build_root: str = Field(
        "",
        description="""
        Local directory where pipeline config JSON and resource files are written during
        build. Defaults to the Laktory cache directory. Use when deployment is delegated
        to third parties like Databricks Declarative Bundles.
        """,
    )

    @model_validator(mode="after")
    def apply_settings(self) -> Any:
        if self.dataframe_backend:
            settings.dataframe_backend = self.dataframe_backend

        if self.workspace_root:
            settings.workspace_root = self.workspace_root

        if self.runtime_root:
            settings.runtime_root = self.runtime_root

        if self.dataframe_api:
            settings.dataframe_api = self.dataframe_api

        if self.build_root:
            settings.build_root = self.build_root

        return self


class StackResources(BaseModel):
    """
    Resources definition for a given stack or stack environment.
    """

    databricks_accesscontrolrulesets: dict[str, AccessControlRuleSet] = {}
    databricks_alerts: dict[str, Alert] = {}
    databricks_apps: dict[str, App] = {}
    databricks_catalogs: dict[str, Catalog] = {}
    databricks_clusterpolicies: dict[str, ClusterPolicy] = {}
    databricks_clusters: dict[str, Cluster] = {}
    databricks_connections: dict[str, Connection] = {}
    databricks_currentusers: dict[str, CurrentUser] = {}
    databricks_dashboards: dict[str, Dashboard] = {}
    databricks_dbfsfiles: dict[str, DbfsFile] = {}
    databricks_directories: dict[str, Directory] = {}
    databricks_entitlements: dict[str, Entitlements] = {}
    databricks_pipelines: dict[str, DatabricksPipeline] = {}
    databricks_externallocations: dict[str, ExternalLocation] = {}
    databricks_grant: dict[str, Grant] = {}
    databricks_grants: dict[str, Grants] = {}
    databricks_groups: dict[str, Group] = {}
    databricks_jobs: dict[str, Job] = {}
    databricks_metastoreassignments: dict[str, MetastoreAssignment] = {}
    databricks_metastoredataaccesses: dict[str, MetastoreDataAccess] = {}
    databricks_metastores: dict[str, Metastore] = {}
    databricks_mlflowexperiments: dict[str, MLflowExperiment] = {}
    databricks_mlflowmodels: dict[str, MLflowModel] = {}
    databricks_mlflowwebhooks: dict[str, MLflowWebhook] = {}
    databricks_networkconnectivityconfig: dict[str, MwsNetworkConnectivityConfig] = {}
    databricks_notebooks: dict[str, Notebook] = {}
    databricks_notificationdestinations: dict[str, NotificationDestination] = {}
    databricks_obotokens: dict[str, OboToken] = {}
    databricks_permissions: dict[str, Permissions] = {}
    databricks_qualitymonitors: dict[str, QualityMonitor] = {}
    databricks_pythonpackages: dict[str, PythonPackage] = {}
    databricks_queries: dict[str, Query] = {}
    databricks_recipients: dict[str, Recipient] = {}
    databricks_repos: dict[str, Repo] = {}
    databricks_schemas: dict[str, Schema] = {}
    databricks_secrets: dict[str, Secret] = {}
    databricks_secretscopes: dict[str, SecretScope] = {}
    databricks_serviceprincipals: dict[str, ServicePrincipal] = {}
    databricks_shares: dict[str, Share] = {}
    databricks_storagecredentials: dict[str, StorageCredential] = {}
    databricks_tables: dict[str, Table] = {}
    databricks_users: dict[str, User] = {}
    databricks_vectorsearchendpoints: dict[str, VectorSearchEndpoint] = {}
    databricks_vectorsearchindexes: dict[str, VectorSearchIndex] = {}
    databricks_volumes: dict[str, Volume] = {}
    databricks_warehouses: dict[str, Warehouse] = {}
    databricks_workspacebindings: dict[str, WorkspaceBinding] = {}
    databricks_workspacefiles: dict[str, WorkspaceFile | PythonPackage] = {}
    databricks_workspacetrees: dict[str, WorkspaceTree | PythonPackage] = {}
    pipelines: dict[str, Pipeline] = {}
    providers: dict[str, AWSProvider | AzureProvider | DatabricksProvider] = {}

    @field_validator("providers", mode="before")
    @classmethod
    def route_providers_by_key(cls, v):
        """Use the provider key name (Terraform convention) as the discriminator.

        AWSProvider and DatabricksProvider share fields like `profile` and `token`,
        so Pydantic's union matching is ambiguous. The key name is the explicit
        source of truth: "databricks[.*]" → DatabricksProvider, "aws[.*]" →
        AWSProvider, "azure[rm][.*]" → AzureProvider.
        """
        if not isinstance(v, dict):
            return v
        result = {}
        for key, value in v.items():
            if not isinstance(value, dict):
                result[key] = value
                continue
            base = key.split(".")[0].lower()
            if base == "databricks":
                result[key] = DatabricksProvider.model_validate(value)
            elif base == "aws":
                result[key] = AWSProvider.model_validate(value)
            elif base in ("azure", "azurerm"):
                result[key] = AzureProvider.model_validate(value)
            else:
                result[key] = value
        return result

    @model_validator(mode="after")
    def update_resource_names(self) -> Any:
        for k, r in self._get_all().items():
            if r.resource_options.name and k != r.resource_options.name:
                raise ValueError(
                    f"Provided resource name {r.resource_options.name} does not match provided key {k}"
                )
            r.resource_options.name = k
        return self

    def _get_all(self, providers_excluded=False, providers_only=False):
        resources = {}
        for resource_type in type(self).model_fields.keys():
            if resource_type in ["variables"]:
                continue

            for resource_name, _r in getattr(self, resource_type).items():
                if resource_name in resources.keys():
                    raise ValueError(
                        f"Stack resource names are not unique. '{resource_name}' is already used."
                    )

                if providers_excluded and isinstance(_r, BaseProvider):
                    continue

                if providers_only and not isinstance(_r, BaseProvider):
                    continue

                resources[resource_name] = _r

        return resources


class EnvironmentSettings(BaseModel):
    """
    Settings overwrite for a specific environments
    """

    resources: Any = Field(
        None,
        description="""
    Dictionary of resources to be deployed. Each key should be a resource type and each value should be a dictionary of
    resources who's keys are the resource names and the values the resources definitions.
    """,
    )
    variables: dict[str, Any] = Field(
        None,
        description="Dictionary of variables made available in the resources definition.",
    )
    terraform: Terraform = Field(Terraform(), description="Terraform-specific settings")


class Stack(BaseModel):
    """
    The Stack defines a collection of deployable resources, the deployment
    configuration, some variables and the environment-specific settings.

    Examples
    --------
    ```py
    from laktory import models

    stack = models.Stack(
        name="workspace",
        resources={
            "databricks_pipelines": {
                "pl-stock-prices": {
                    "name": "pl-stock-prices",
                    "development": "${vars.is_dev}",
                    "libraries": [
                        {"notebook": {"path": "/pipelines/dlt_brz_template.py"}},
                    ],
                }
            },
            "databricks_jobs": {
                "job-stock-prices": {
                    "name": "job-stock-prices",
                    "job_clusters": [
                        {
                            "job_cluster_key": "main",
                            "new_cluster": {
                                "spark_version": "16.3.x-scala2.12",
                                "node_type_id": "Standard_DS3_v2",
                            },
                        }
                    ],
                    "tasks": [
                        {
                            "task_key": "ingest",
                            "job_cluster_key": "main",
                            "notebook_task": {
                                "notebook_path": "/.laktory/jobs/ingest_stock_prices.py",
                            },
                        },
                        {
                            "task_key": "pipeline",
                            "depends_on": [{"task_key": "ingest"}],
                            "pipeline_task": {
                                "pipeline_id": "${resources.dlt-pl-stock-prices.id}",
                            },
                        },
                    ],
                }
            },
        },
        variables={
            "org": "okube",
        },
        environments={
            "dev": {
                "variables": {
                    "is_dev": True,
                }
            },
            "prod": {
                "variables": {
                    "is_dev": False,
                }
            },
        },
    )
    ```
    References
    ----------
    * [Stack](https://www.laktory.ai/concepts/stack/)
    """

    iac_backend: Literal["terraform"] = Field(
        "terraform",
        description="IaC backend used for deployment.",
        validation_alias="backend",  # TODO: Supported for backward compatibility
    )
    description: str = Field(None, description="Description of the stack")
    environments: dict[str, EnvironmentSettings] = Field(
        {},
        description="Environment-specific overwrite of config, resources or variables arguments.",
    )
    name: str = Field(
        ...,
        description="Name of the stack.",
    )
    organization: str | None = Field(None, description="Organization")
    resources: StackResources | None = Field(
        StackResources(),
        description="""
    Dictionary of resources to be deployed. Each key should be a resource type and each value should be a dictionary of
    resources who's keys are the resource names and the values the resources definitions.
    """,
    )
    settings: LaktorySettings = Field(None, description="Laktory settings")
    terraform: Terraform = Field(Terraform(), description="Terraform-specific settings")
    variables: dict[str, Any] = Field(
        {},
        description="Dictionary of variables made available in the resources definition.",
    )

    @model_validator(mode="before")
    @classmethod
    def apply_settings(cls, data: Any) -> Any:
        """Required to apply settings before instantiating resources and setting default values"""
        settings = data.get("settings", None)
        if settings:
            if not isinstance(settings, dict):
                settings = settings.model_dump(exclude_unset=True)
            LaktorySettings(**settings)

        return data

    # ----------------------------------------------------------------------- #
    # Methods                                                                 #
    # ----------------------------------------------------------------------- #

    def build(self, env_name: str | None, inject_vars: bool = True, vars: dict = None):
        """
        Build stack artifacts before preview or deploy.

        Pipeline config JSON files are written to the location determined by
        ``settings.build_root`` (when set in ``stack.yaml`` under
        ``settings:``) or the default Laktory cache directory. For Databricks
        Asset Bundles users, set ``settings.build_root`` to a
        project-local path (e.g. ``.laktory/.resources/``) so that DABs can
        sync the files to the workspace.

        Parameters
        ----------
        env_name:
            Name of the environment
        inject_vars:
            Inject stack variables
        vars:
            Additional variables that override stack and environment variables.
        """

        logger.info("Building artifacts...")

        env = self.get_env(env_name=env_name)
        if inject_vars:
            if vars:
                env = env.model_copy(update={"variables": {**env.variables, **vars}})
            env = env.inject_vars()

        if env.resources is None:
            return

        for k, r in env.resources._get_all(providers_excluded=True).items():
            if isinstance(r, PythonPackage):
                r.build()

        logger.info("Writing pipeline config files...")
        for k, r in env.resources._get_all(providers_excluded=True).items():
            if isinstance(r, Pipeline):
                orchestrator = r.orchestrator
                if not orchestrator:
                    continue

                config_file = getattr(r.orchestrator, "config_file", None)
                if config_file:
                    config_file.build()

        logger.info("Build completed.")

    def _apply_env_overrides(self, env: EnvironmentSettings) -> "Stack":
        """
        Return a new Stack where *env*'s settings are merged on top of this
        stack's defaults.

        Merge rules
        -----------
        - ``variables``: key-by-key merge; env values take precedence over
          stack defaults.  Keys present only in the stack are preserved; keys
          present only in the env are added; keys present in both use the env
          value.
        - All other fields (``resources``, ``terraform``, …): env value
          replaces the stack default entirely when set.
        - ``environments`` is cleared on the returned copy (already resolved).
        """
        merged = self.model_copy(update={"environments": {}})
        merged.update(env.model_dump(exclude_unset=True))
        return merged

    def get_env(self, env_name: str | None):
        """
        Complete definition the stack for a given environment. It takes into
        account both the default stack values and environment-specific
        overwrites.

        Parameters
        ----------
        env_name:
            Name of the environment

        Returns
        -------
        :
            Environment definitions.
        """

        if env_name is None:
            return self

        if env_name not in self.environments.keys():
            raise ValueError(f"Environment '{env_name}' is not declared in the stack.")

        return self._apply_env_overrides(self.environments[env_name])

    # ----------------------------------------------------------------------- #
    # Helpers                                                                 #
    # ----------------------------------------------------------------------- #

    @staticmethod
    def _check_depends_on(resources: dict, providers: dict) -> None:
        """Warn when a depends_on entry references a ${resources.X} name that
        does not exist in the stack, catching typos before Terraform apply."""
        known = set(resources) | set(providers)
        pattern = re.compile(r"\$\{resources\.([^}.]+)")
        for _r in resources.values():
            for dep in _r.resource_options.depends_on:
                m = pattern.search(dep)
                if m and m.group(1) not in known:
                    logger.warning(
                        f"Resource '{_r.resource_options.name}' has depends_on "
                        f"entry '{dep}' that references unknown resource '{m.group(1)}'."
                    )

    # ----------------------------------------------------------------------- #
    # Terraform Methods                                                       #
    # ----------------------------------------------------------------------- #

    def to_terraform(self, env_name: str | None = None, vars: dict = None):
        """
        Create a terraform stack for a given environment `env`.

        Parameters
        ----------
        env_name:
            Target environment. If `None`, used default stack values only.
        vars:
            Additional variables that override stack and environment variables.

        Returns
        -------
        : TerraformStack
            Terraform-specific stack definition
        """
        from laktory.models.stacks.terraformstack import TerraformStack

        env = self.get_env(env_name=env_name)
        if vars:
            env = env.model_copy(update={"variables": {**env.variables, **vars}})
        env = env.inject_vars()
        env.build(env_name=None, inject_vars=False)

        # Providers
        providers = {}
        for r in env.resources._get_all(providers_only=True).values():
            for _r in r.core_resources:
                rname = _r.resource_name
                providers[rname] = _r

        # Resources
        resources = {}
        for r in env.resources._get_all(providers_excluded=True).values():
            for _r in r.core_resources:
                resources[_r.resource_name] = _r

        self._check_depends_on(resources, providers)

        # Auto-configure Databricks workspace state backend when
        # backend.databricks_workspace is set.
        ws_backend = None
        if env.terraform.backend and "databricks_workspace" in env.terraform.backend:
            ws_backend = env.terraform.backend.pop("databricks_workspace")
            if not env.terraform.backend:
                env.terraform.backend = None
            if ws_backend is False:
                ws_backend = None

        if ws_backend is not None:
            if env.terraform.backend:
                logger.warning(
                    "'databricks_workspace' is set alongside other backend keys; "
                    "the other keys take precedence and databricks_workspace is ignored."
                )
            else:
                db_provider = next(
                    (
                        p
                        for p in providers.values()
                        if isinstance(p, DatabricksProvider)
                    ),
                    None,
                )
                if db_provider is None:
                    raise ValueError(
                        "backend.databricks_workspace requires a DatabricksProvider in the stack."
                    )
                wc = db_provider.workspace_client
                host = (db_provider.host or wc.config.host or "").rstrip("/")
                if not host:
                    raise ValueError(
                        "Could not resolve Databricks host for backend.databricks_workspace. "
                        "Set 'host' explicitly in the DatabricksProvider or ensure the profile "
                        "in ~/.databrickscfg includes a host."
                    )

                username = wc.current_user.me().user_name

                # The Terraform HTTP backend uses Basic auth (Authorization:
                # Basic base64("token:{password}")). Databricks accepts this
                # for PAT tokens but NOT for Azure AD / OAuth tokens, which
                # require Bearer auth. When no PAT is configured, create a
                # Databricks PAT via the Tokens API and cache it on disk so the
                # backend configuration stays stable across laktory init / deploy
                # invocations. The cache is scoped to CACHE_ROOT (next to
                # stack.tf.json) and must be kept out of source control.
                if db_provider.token:
                    token = db_provider.token
                else:
                    from laktory.constants import CACHE_ROOT

                    cache_path = os.path.join(CACHE_ROOT, ".terraform", _WS_TOKEN_CACHE)
                    try:
                        token = _get_cached_ws_token(wc, cache_path)
                    except Exception as exc:
                        raise ValueError(
                            "backend.databricks_workspace: could not create or "
                            "retrieve a Databricks PAT token for the Terraform "
                            "HTTP backend. This is required when using service "
                            "principal authentication because Azure AD tokens are "
                            "not accepted via Basic auth. Ensure the service "
                            "principal has permission to create tokens (workspace "
                            "Admin Console > Settings > Advanced > 'Allow service "
                            "principals to use personal access tokens'), or set an "
                            "explicit 'token' in your DatabricksProvider. "
                            f"Original error: {exc}"
                        ) from exc

                if ws_backend is not True:
                    raise ValueError(
                        "backend.databricks_workspace must be set to `true`. "
                        "To use a fully custom backend, configure the `http` backend directly."
                    )

                if env_name:
                    state_path = f"/Users/{username}/.laktory/{self.name}/{env_name}/state/terraform.tfstate"
                else:
                    state_path = f"/Users/{username}/.laktory/{self.name}/state/terraform.tfstate"
                ws_parent = state_path.rsplit("/", 1)[0]
                wc.workspace.mkdirs(path=ws_parent)

                url = f"{host}/api/2.0/workspace-files{state_path}"
                env.terraform.backend = {
                    "http": {
                        "address": url,
                        "username": "token",
                        "password": token,
                        "retry_wait_min": "5",
                    }
                }

        # Update terraform
        return TerraformStack(
            terraform={"backend": env.terraform.backend},
            providers=providers,
            resources=resources,
        )
