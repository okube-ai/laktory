??? "API Documentation"
    [`laktory.cli.app.app`][laktory.cli.app.app]<br>

When Laktory is pip installed, it also installs the Laktory CLI that can be invoked from the terminal.

```cmd
laktory --help
```
The CLI supports 4 main commands `preview`, `deploy`, `run` and `destroy` providing full support for common CI/CD
operations, whether locally from the terminal or using your favorite a CI/CD tool like GitHub actions, Azure DevOps or 
Gitlab.

The CLI also offers a `quickstart` command for quickly setting up a working example of a Laktory stack.

### commands

#### quickstart
`laktory quickstart` setup a working example of a deployable stack that you can use as a baseline. See [Quickstart](/quickstart) for more details.

#### init
`laktory init` setups IaC backend and download required resources. Only available with Terraform backend.

#### preview
`laktory preview` validates your yaml stack and describe new resources to be deployed or changes to existing ones.  Similar to `pulumi preview` or `terraform validate/plan`.

#### deploy
`laktory deploy` executes the deployment by creating or updating resources.  Similar to `pulumi up` or `terraform apply`.

#### run
`laktory run` execute remote job or DLT pipeline and monitor failures until completion. Local execution (without an orchestrator) of a pipeline is not yet supported.

#### destroy
`laktory destroy` destroy all resources declared in your stack. Similar to `pulumi destroy` or `terraform destroy`

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

Here is what the laktory-deploy workflow could look like
```yaml
name: laktory-deploy

[...]

jobs:
  laktory-deploy-dev:
    uses: ./.github/workflows/_job_laktory_deploy.yml
    with:
      env: dev
      working-directory: ./workspace
      databricks_host: 'adb-4623853922539974.14.azuredatabricks.net'
    secrets: inherit

  laktory-run-dev:
    needs: laktory-deploy-dev
    uses: ./.github/workflows/_job_laktory_run_pipeline.yml
    with:
      env: dev
      working-directory: ./workspace
      databricks_host: 'adb-4623853922539974.azuredatabricks.net'
      pipeline_name: pl-stock-prices
    secrets: inherit

  laktory-preview-prd:
    needs: laktory-run-dev
    uses: ./.github/workflows/_job_laktory_preview.yml
    with:
      env: prd
      working-directory: ./workspace
    secrets: inherit

  prd-deploy-approval:
    needs: laktory-preview-prd
    uses: ./.github/workflows/_job_release_approval.yml
    secrets: inherit

  laktory-deploy-prd:
    needs: prd-deploy-approval
    uses: ./.github/workflows/_job_laktory_deploy.yml
    with:
      env: prd
      working-directory: ./workspace
      databricks_host: 'adb-1985337240298151.azuredatabricks.net'
    secrets:
      pulumi_access_token: ${{ secrets.PULUMI_ACCESS_TOKEN }}
      azure_client_id: ${{ secrets.AZURE_CLIENT_ID_PRD }}
      azure_client_secret: ${{ secrets.AZURE_CLIENT_SECRET_PRD }}
      azure_tenant_id: ${{ secrets.AZURE_TENANT_ID }}
```

Of course, the workflows can be customized for each project specific requirements, but they all generally require to
use the `preview`, `deploy` and `run` CLI commands.
