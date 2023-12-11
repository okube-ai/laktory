import pytest
from pytest_examples import find_examples, CodeExample, EvalExample


@pytest.mark.parametrize(
    "example", find_examples("./laktory/spark/dataframe"), ids=str
)
def test_docstrings_spark_dataframe(example: CodeExample, eval_example: EvalExample):
    if eval_example.update_examples:
        eval_example.format(example)
        eval_example.run_print_update(example)
    else:
        print(eval_example.config)
        eval_example.lint(example)
        eval_example.run_print_check(example)


@pytest.mark.parametrize(
    "example", find_examples("./laktory/spark/functions"), ids=str
)
def test_docstrings_spark_functions(example: CodeExample, eval_example: EvalExample):
    if eval_example.update_examples:
        eval_example.format(example)
        eval_example.run_print_update(example)
    else:
        print(eval_example.config)
        eval_example.lint(example)
        eval_example.run_print_check(example)

