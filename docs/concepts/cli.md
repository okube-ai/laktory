??? "API Documentation"
    [`laktory.cli.app`][laktory.cli.app]<br>

When Laktory is pip installed, it also installs the Laktory CLI that can be invoked from the terminal.

```cmd
laktory --help
```
The CLI supports 3 main operations `init`, `preview` and `deploy`.

### init
**This command is under development and should be released soon**
`laktory init` helps you setup a yaml stack and some sample resources to get your started.

### preview
`laktory preview` validate your yaml stack and describe new resources to be deployed or changes to existing ones.  Similar to `pulumi preview` or `terraform validate/plan`.

### deploy
`laktory deploy` execute the deployment by creating or updating resources.  Similar to `pulumi up` or `terraform apply`.
