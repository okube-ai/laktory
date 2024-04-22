??? "API Documentation"
    [`laktory.cli.app.app`][laktory.cli.app.app]<br>

When Laktory is pip installed, it also installs the Laktory CLI that can be invoked from the terminal.

```cmd
laktory --help
```
The CLI supports 3 main commands `preview`, `deploy` and `run` providing full support for common CI/CD operations, 
whether locally from the terminal or using your favorite a CI/CD tool like GitHub actions, Azure DevOps or Gitlab.

The CLI also offers a `quickstart` command for quickly setting up a working example of a Laktory stack.

### commands

#### quickstart
`laktory quickstart` setup a working example of a stock prices pipeline that you can use as a baseline. See [Quickstart](/quickstart) for more details.

#### init
`laktory init` setups IaC backend and download required resources. Only available with Terraform backend.

#### preview
`laktory preview` validates your yaml stack and describe new resources to be deployed or changes to existing ones.  Similar to `pulumi preview` or `terraform validate/plan`.

#### deploy
`laktory deploy` executes the deployment by creating or updating resources.  Similar to `pulumi up` or `terraform apply`.

#### run
`laktory run` execute remote job or pipeline and monitor failures until completion.

### CI/CD
These commands can be run locally, but really start to provide value in the context of a CI/CD pipeline in which 
complex testing, validation and deployment flows can be built. An example of such workflows is provided in the 
[lakehouse-as-code](https://github.com/okube-ai/lakehouse-as-code/tree/cli_run/.github/workflows) repository.

In this case, we have 2 workflows:
- laktory-preview
  - triggered during a pull-request
  - preview the changes in `dev` environment
  - preview successful for merging pull-request
- laktory-deploy
  - triggered after merge to main branch
  - deploy changes to `dev` environment
  - run pipeline in `dev` environment and monitor for failures
  - preview changes to `prod` environment
  - request for manual approval
  - deploy changes to `prod` environment

Of course, the workflows can be customized for each project specific requirements, but they all generally require to
use the `preview`, `deploy` and `run` CLI commands.
