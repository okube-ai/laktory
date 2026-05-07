from laktory.models.resources.terraformresource import TerraformResource


class VirtualTerraformResource(TerraformResource):
    """
    Base class for Laktory models that participate in the resource graph only
    as containers. They never emit a Terraform resource block of their own but
    generate child resources via ``additional_core_resources``.

    Concrete examples: ``WorkspaceTree``, ``Pipeline``.
    """

    __doc_hide_base__ = True

    @property
    def terraform_resource_type(self) -> None:
        return None

    @property
    def self_as_core_resources(self) -> bool:
        return False
