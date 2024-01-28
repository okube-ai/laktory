??? "API Documentation"
    [`laktory.cli.app`][laktory.cli.app]<br>

When Laktory is pip installed, it also installs the Laktory CLI that can be invoked from the terminal.

```cmd
laktory --help
```
The CLI supports 3 main operations `init`, `preview` and `deploy`.

### init
`laktory init` setups IaC backend and download required resources. Only available with Terraform backend.

### preview
`laktory preview` validate your yaml stack and describe new resources to be deployed or changes to existing ones.  Similar to `pulumi preview` or `terraform validate/plan`.

### deploy
`laktory deploy` execute the deployment by creating or updating resources.  Similar to `pulumi up` or `terraform apply`.
