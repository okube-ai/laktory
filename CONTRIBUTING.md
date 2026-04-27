# Contributing

Thank you for your interest in contributing to Laktory! Any kind of improvement is welcome.

This guide will help you get started and outline how to best contribute to the project.

## Before You Begin

- **Familiarize Yourself with Laktory**  
   - Read the [README](README.md) to understand the project's goals and features.
   - Explore the [Installation Guide](https://www.laktory.ai/install/) to get Laktory up and running locally.
   - Review the [documentation](https://www.laktory.ai/) for in-depth details on its functionality.

- **Join the Community**  
   - Connect with other contributors and maintainers on our [Slack channel](http://okube.slack.com/).

- **New Features or Significant Changes**:  
  - If you're considering significant changes, [submit an issue](https://github.com/okube-ai/laktory/issues/new/choose) first. This ensures alignment with the project's direction and avoids working on features that may not be merged.

- **Fixing Bugs**:  
  - **Check Existing Issues**: Search [open issues](https://github.com/okube-ai/laktory/issues) to see if the bug has already been reported.
    - If **not reported**, [create a new issue](https://github.com/okube-ai/laktory/issues/new/choose). You're welcome to fix it and submit a pull request with your solution. Thank you!
    - If the bug is **already reported**, leave a comment stating you're working on fixing it. This keeps everyone updated and avoids duplicate efforts.

## Getting Started

### 1. Fork the Repository

Go to the [main project page](https://github.com/okube-ai/laktory).
Fork the repository by clicking the fork button in the top-right corner of the page.

### 2. Clone the Repository

Go to the forked repository on your GitHub account (under the Repositories tab).
Click the green `Code` button and copy the URL to your clipboard.

Open a terminal, navigate to your desired directory, and run:

```bash
git clone <url you just copied>
```

For example:

```bash
git clone git@github.com:YOUR-GITHUB-USERNAME/laktory.git laktory-dev
```

Then, navigate to the cloned repository:

```bash
cd laktory-dev
```

### 3. Set Up the Environment with UV

1. Ensure you have Python 3.12 installed. Create and activate a virtual environment. Here's one recommended method:
   - Install UV: [Getting Started with UV](https://github.com/astral-sh/uv?tab=readme-ov-file#getting-started)
     or update it with:
     ```
     uv self update
     ```
   - Install Python 3.12:
     ```
     uv python install 3.12
     ```

2. Install Laktory:
   ```
   make install-dev
   ```
   This includes core dependencies, unit testing tools, and formatting/linting packages.

### 4. Create a Branch

Create a new branch from the `main` branch in your local repository. Use this naming convention:

```
{category}/{issue-id}-description-of-the-branch
# Example:
fix/231-spark-json-read
```

#### Branch Categories
- `feat` - New feature implementation (requires a ticket).
- `fix` - Bug fixes (requires a ticket).
- `exp` - Experimental changes or demonstrations (ticket encouraged).
- `test` - Changes related to testing (ticket encouraged).
- `docs` - Documentation updates (ticket optional).

### 5. Code and Test

Once your changes are ready, run the tests using:

```bash
make test
```

If you add new functionality, include appropriate tests.

#### Testing Spark

Setting up the dev environment installs PySpark. For local Spark execution, follow [these instructions](https://www.machinelearningplus.com/pyspark/install-pyspark-on-mac/).

If you use Homebrew for Java installation, set these environment variables:
- `JAVA_HOME=/opt/homebrew/opt/java`
- `SPARK_HOME=/opt/homebrew/Cellar/apache-spark/3.5.0/libexec`

### 6. Format and Lint

We use Ruff for code formatting and linting. Run:

```bash
make format-and-lint
```

This automatically reformats code and flags linting violations. These rules are also applied as pre-commit actions.

### 7. Update the Changelog

Document your changes in `CHANGELOG.md`.

### 8. Build the Documentation

To build the docs, run:

```bash
mkdocs serve
```

Open the provided link in your browser. If the docs don't refresh after changes, stop the server with `Ctrl+C`, rebuild using `mkdocs build`, and serve again.

### 9. Open a Pull Request

When ready, [open a pull request](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request-from-a-fork) in the Laktory repository.

Follow these guidelines:

1. Start your pull request title with one of these prefixes: `[feat]`, `[fix]`, `[exp]`, `[test]`, `[docs]`.
2. Complete the pull request form and submit.

## Databricks Resource Model Generation

The 50+ classes under `laktory/models/resources/databricks/` that end in `_base.py`
(e.g. `catalog_base.py`, `job_base.py`) are **auto-generated** from the Databricks
Terraform provider schema. Do not edit them by hand — they will be overwritten on the
next run.

### Scripts

All scripts live in `scripts/build_resources/`:

| Script             | Purpose                                                                                                                                                                |
|--------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `_config.py`       | Shared constants (`DEFAULT_TARGETS`, `RESOURCE_NAME_OVERRIDES`) and naming helpers (`resource_to_class_name`, `base_file_stem`). Imported by the other scripts.        |
| `00_fetch.py`      | Downloads the Terraform provider schema and fetches field descriptions from GitHub. Writes `databricks_schema.json` and `databricks_descriptions.json`.                |
| `01_build.py`      | Reads the schema + descriptions and generates one `*_base.py` file per resource. Runs `ruff format` + `ruff check --fix` on the output, then calls `02_update_api.py`. |
| `02_update_api.py` | Updates the MkDocs API stub `.md` files under `docs/api/models/resources/databricks/` to reflect the latest base classes and override files.                           |

### When to regenerate

Regenerate when upgrading the Databricks Terraform provider version (set in `DEFAULT_VERSION` inside `00_fetch.py`):

```bash
# Step 1 — fetch schema + descriptions for the new provider version
python scripts/build_resources/00_fetch.py 1.120.0

# Step 2 — regenerate *_base.py files and update API docs
#           (02_update_api.py is called automatically at the end)
python scripts/build_resources/01_build.py
```


## Happy Contributing!

Remember to abide by the code of conduct, or you may be kindly escorted out of the project.
