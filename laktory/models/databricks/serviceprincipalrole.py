from laktory.models.basemodel import BaseModel
from laktory.models.resources.pulumiresource import PulumiResource


class ServicePrincipalRole(BaseModel, PulumiResource):
    """
    Databricks Service Principal role

    Attributes
    ----------
    role:
        This is the id of the role or instance profile resource.
    service_principal_id:
        This is the id of the service principal resource.
    """

    role: str = None
    service_principal_id: str = None

    # ----------------------------------------------------------------------- #
    # Resource Properties                                                     #
    # ----------------------------------------------------------------------- #

    # ----------------------------------------------------------------------- #
    # Pulumi Properties                                                       #
    # ----------------------------------------------------------------------- #

    @property
    def pulumi_resource_type(self) -> str:
        return "databricks:ServicePrincipalRole"

    @property
    def pulumi_cls(self):
        import pulumi_databricks as databricks

        return databricks.ServicePrincipalRole
