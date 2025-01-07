# Contributing

Thank you for your interest in contributing to Laktory! Any kind of improvement is welcome!

This guide will help you get started and outline how to best contribute to the project.

## Before You Begin

- **Familiarize Yourself with Laktory**  
   - Read the [README](README.md) to understand the project's goals and features.
   - Explore the [Installation Guide](https://www.laktory.ai/install/) to get Laktory up and running locally.
   - Review the [documentation](https://www.laktory.ai/) for in-depth details on its functionality.

- **Join the Community**  
   - Connect with other contributors and maintainers on our [Slack channel](http://okube.slack.com/).

- **New features of significant changes**: 
  - If you're thinking about making significant changes, make sure to [submit an issue](https://github.com/okube-ai/laktory/issues/new/choose) first. This ensures your efforts align with the project's direction and that you don't invest time on a feature that may not be merged.

- **Fixing bugs**:
  - **Check existing issues**: search [open issues](https://github.com/okube-ai/laktory/issues) to see if the bug you've found is already reported.
    - If **not reported**, [create a new issue](https://github.com/okube-ai/laktory/issues/new/choose). You're more than welcome to fix it and submit a pull request with your solution. Thank you!
    - If the bug is **already reported**, please leave a comment on that issue stating you're working on fixing it. This helps keep everyone updated and avoids duplicate efforts.

## Getting Started

### 1. Fork the repository

Go to the [main project page](https://github.com/okube-ai/laktory).
Fork the repository by clicking on the fork button. You can find it in the right corner on the top of the page.

### 2. Clone the repository

Go to the forked repository on your GitHub account - you'll find it on your account in the tab Repositories. 
Click on the green `Code` button and then click the `Copy url to clipboard` icon.
Open a terminal, choose the directory where you would like to have Narwhals repository and run the following git command:

```bash
git clone <url you just copied>
``` 

for example:

```bash
git clone git@github.com:YOUR-GITHUB-USERNAME/laktory.git laktory-dev
```

You should then navigate to the folder you just created:

```bash
cd laktory-dev
```

### 3: Setup Environment with UV

1. Make sure you have Python3.12 installed, create a virtual environment,
   and activate it. If you're new to this, here's one way that we recommend:
   1. Install uv: https://github.com/astral-sh/uv?tab=readme-ov-file#getting-started
      or make sure it is up-to-date with:
      ```
      uv self update
      ```
   2. Install Python3.12:
      ```
      uv python install 3.12
      ```
2. Install Laktory
   ```
   make install-dev
   ```
   This will include core dependencies, but also all packages required to run unit tests and to format/lint the code.


### 4. Create a Branch

Create a new git branch from the `main` branch in your local repository following this
naming convention:
```
{category}/{issue-id}-description-of-the-branch
# example:
fix/231-spark-json-read
```

#### Branch categories
- feat - a new feature that is being implemented (ticket required)
- fix - a change that fixes a bug (ticket required)
- exp - an experiment where we are testing a new idea or want to demonstrate something to the team, might turn into a feat later (ticket encouraged)
- test - anything related to the tests (ticket encouraged)
- docs - a change to our docs (ticket optional)


### 5. Code and Test

Once your changes are ready, make sure to run the tests using
```
make test
```
If you add code that should be tested, please add tests.


#### Testing Spark

Setting up the dev environment described above will install pyspark. However, for running 
spark locally, you also need to follow instructions provided [here](https://www.machinelearningplus.com/pyspark/install-pyspark-on-mac/).
If you use homebrew to install java, your JAVA_HOME and SPARK_HOME environment variables should look something like:
- JAVA_HOME=/opt/homebrew/opt/java
- SPARK_HOME=/opt/homebrew/Cellar/apache-spark/3.5.0/libexec

### 6. Format and Lint

To keep the code clean and tidy, we use ruff for both code formatting and linting. Simply run
```
make format-and-lint
```
from the Laktory root folder. This will automatically re-format the code and identify
any linting violation.

The same rules are applied automatically as a pre-commit action.

### 7. Update CHANGELOG
Update CHANGELOG.md to describe the proposed changes.

### 7. Building docs

To build the docs, run `mkdocs serve`, and then open the link provided in a browser.
The docs should refresh when you make changes. If they don't, press `ctrl+C`, and then
do `mkdocs build` and then `mkdocs serve`.

### 8. Pull requests

When you have resolved your issue, [open a pull request](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request-from-a-fork) in the Laktory repository.

Please adhere to the following guidelines:

1. Start your pull request title with one of these prefixes a `[feat]`, `[fix]`, `[exp]`, `[test]`, `[doc]`.
2. Please follow the instructions in the pull request form and submit. 

## Happy contributing!

Please remember to abide by the code of conduct, else you'll be conducted away from this project.
