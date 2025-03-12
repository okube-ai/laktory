from laktory.models.resources.databricks import WorkspaceBinding

workspace_binding = WorkspaceBinding(
    securable_name="credential",
    workspace_id="11111111",
    securable_type="catalog",
    binding_type="BINDING_TYPE_READ_WRITE"
)


def test_workspace_binding():
    print(workspace_binding)
    assert workspace_binding.securable_name == "credential"
    assert workspace_binding.workspace_id == "11111111"
    assert workspace_binding.securable_type == "catalog"
    assert workspace_binding.binding_type == "BINDING_TYPE_READ_WRITE"
    assert workspace_binding.resource_key == "credential-11111111"
    assert (
        workspace_binding.resource_name
        == "workspace-binding-credential-11111111"
    )



if __name__ == "__main__":
    test_workspace_binding()
