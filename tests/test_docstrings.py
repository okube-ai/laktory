import os
import re
from pathlib import Path

import pytest
import pytest_examples
from pytest_examples import CodeExample
from pytest_examples import EvalExample
from pytest_examples import find_examples
from pytest_examples.lint import black_format as default_black_format

import laktory as lk

# Change examples print prefix to be ruff lint compatible
comment_prefix = "# > "
pytest_examples.run_code.comment_prefix = comment_prefix
pytest_examples.run_code.comment_prefix_re = re.compile(
    f"^ *{re.escape(comment_prefix)}", re.MULTILINE
)


# Overwrite black format to support Narwhals DataFrame print
# ┌──────────────────┐
# |Narwhals DataFrame|
# |------------------|
# | | x | y1 | y2 |  |
# | |---|----|----|  |
# | | 1 | 2  | 3  |  |
# | | 2 | 3  | 5  |  |
# | | 3 | 4  | 7  |  |
# └──────────────────┘
def black_format(source: str, *args, **kwargs) -> str:
    lines = []
    for i, line in enumerate(source.splitlines()):
        if line.strip().startswith("┌─"):
            line = ""
        if line.strip().startswith("└─"):
            line = ""
        lines += [line]
    return default_black_format("\n".join(lines), *args, **kwargs)


pytest_examples.lint.black_format = black_format
pytest_examples.run_code.black_format = black_format

# Simplify polars dataframe print output
os.environ["POLARS_FMT_TABLE_HIDE_COLUMN_DATA_TYPES"] = "1"
os.environ["POLARS_FMT_TABLE_HIDE_DATAFRAME_SHAPE_INFORMATION"] = "1"
os.environ["POLARS_FMT_TABLE_FORMATTING"] = "MARKDOWN"


# --------------------------------------------------------------------------- #
# Root functions                                                              #
# --------------------------------------------------------------------------- #

root = Path(__file__).parent.parent / "laktory"
files = [root / fname for fname in os.listdir(root) if fname.endswith(".py")]


@pytest.mark.parametrize("example", find_examples(*files), ids=str)
def test_docstrings_root(example: CodeExample, eval_example: EvalExample):
    if eval_example.update_examples:
        eval_example.format_ruff(example)
        if "tag:skip-run" not in example.prefix_tags():
            eval_example.run_print_update(example)
    else:
        eval_example.lint_ruff(example)
        if "tag:skip-run" not in example.prefix_tags():
            eval_example.run_print_check(example)


# --------------------------------------------------------------------------- #
# Dispatcher                                                                  #
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("example", find_examples("./laktory/dispatcher/"), ids=str)
def test_docstrings_dispatcher(example: CodeExample, eval_example: EvalExample):
    if eval_example.update_examples:
        eval_example.format_ruff(example)
        if "tag:skip-run" not in example.prefix_tags():
            eval_example.run_print_update(example)
    else:
        eval_example.lint_ruff(example)
        if "tag:skip-run" not in example.prefix_tags():
            eval_example.run_print_check(example)


# --------------------------------------------------------------------------- #
# Models                                                                      #
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("example", find_examples("./laktory/models"), ids=str)
def test_docstrings_models(example: CodeExample, eval_example: EvalExample):
    if eval_example.update_examples:
        pass
        eval_example.format_ruff(example)
        if "tag:skip-run" not in example.prefix_tags():
            eval_example.run_print_update(
                example,
                module_globals={"spark": lk.get_spark_session()},
            )
    else:
        eval_example.lint_ruff(example)
        if "tag:skip-run" not in example.prefix_tags():
            eval_example.run_print_check(
                example, module_globals={"spark": lk.get_spark_session()}
            )


# --------------------------------------------------------------------------- #
# Narwhals Extension                                                          #
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("example", find_examples("./laktory/narwhals_ext"), ids=str)
def test_docstrings_narwhals_ext(example: CodeExample, eval_example: EvalExample):
    if eval_example.update_examples:
        eval_example.format_ruff(example)
        if "tag:skip-run" not in example.prefix_tags():
            eval_example.run_print_update(example)
    else:
        eval_example.lint_ruff(example)
        if "tag:skip-run" not in example.prefix_tags():
            eval_example.run_print_check(example)


# --------------------------------------------------------------------------- #
# yaml                                                                        #
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("example", find_examples("./laktory/yaml"), ids=str)
def test_docstrings_yaml(example: CodeExample, eval_example: EvalExample):
    if eval_example.update_examples:
        eval_example.format_ruff(example)
        if "tag:skip-run" not in example.prefix_tags():
            eval_example.run_print_update(example)
    else:
        eval_example.lint_ruff(example)
        if "tag:skip-run" not in example.prefix_tags():
            eval_example.run_print_check(example)


# --------------------------------------------------------------------------- #
# Markdowns                                                                   #
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("example", find_examples("./docs/"), ids=str)
def test_docstrings_markdowns(example: CodeExample, eval_example: EvalExample):
    """
    Examples in documentation are not designed to be run in isolation. We only format
    them. To be investigated if there is a better approach.
    """

    if eval_example.update_examples:
        eval_example.format_ruff(example)
    else:
        eval_example.lint_ruff(example)
