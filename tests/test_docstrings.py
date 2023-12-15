import pytest
from pytest_examples import find_examples, CodeExample, EvalExample
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()


# --------------------------------------------------------------------------- #
# DLT                                                                         #
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("example", find_examples("./laktory/dlt"), ids=str)
def test_docstrings_dlt(example: CodeExample, eval_example: EvalExample):
    if eval_example.update_examples:
        eval_example.format(example)
        eval_example.run_print_update(
            example,
            module_globals={
                "spark": spark,
                "display": lambda x: x,
            },
        )
    else:
        eval_example.lint(example)
        eval_example.run_print_check(
            example,
            module_globals={
                "spark": spark,
                "display": lambda x: x,
            },
        )


# --------------------------------------------------------------------------- #
# Models                                                                      #
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("example", find_examples("./laktory/models"), ids=str)
def test_docstrings_models(example: CodeExample, eval_example: EvalExample):
    if eval_example.update_examples:
        eval_example.format(example)
        eval_example.run_print_update(
            example,
            module_globals={"spark": spark},
        )
    else:
        eval_example.lint(example)
        eval_example.run_print_check(example, module_globals={"spark": spark})


# --------------------------------------------------------------------------- #
# Spark DataFrame                                                             #
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("example", find_examples("./laktory/spark/dataframe"), ids=str)
def test_docstrings_spark_dataframe(example: CodeExample, eval_example: EvalExample):
    if eval_example.update_examples:
        eval_example.format(example)
        eval_example.run_print_update(example, module_globals={"spark": spark})
    else:
        eval_example.lint(example)
        eval_example.run_print_check(example, module_globals={"spark": spark})


# --------------------------------------------------------------------------- #
# Spark Functions                                                             #
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("example", find_examples("./laktory/spark/functions"), ids=str)
def test_docstrings_spark_functions(example: CodeExample, eval_example: EvalExample):
    if eval_example.update_examples:
        eval_example.format(example)
        eval_example.run_print_update(example, module_globals={"spark": spark})
    else:
        eval_example.lint(example)
        eval_example.run_print_check(example, module_globals={"spark": spark})


# --------------------------------------------------------------------------- #
# Markdowns                                                                   #
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("example", find_examples("./docs/"), ids=str)
def test_docstrings_markdowns(example: CodeExample, eval_example: EvalExample):
    """
    Examples in documentation are not all designed to be run in isolation. We only format them using black. To be
    investigated if there is a better approach.
    """

    if eval_example.update_examples:
        eval_example.format_black(example)
    else:
        eval_example.lint_black(example)
