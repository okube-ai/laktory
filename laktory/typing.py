from typing import TYPE_CHECKING

import narwhals as nw
from pydantic_core import CoreSchema
from pydantic_core import core_schema

if TYPE_CHECKING:
    pass

AnyFrame = nw.LazyFrame | nw.DataFrame


class VariableType(str):
    """
    A string placeholder that defers field resolution to `inject_vars()`.

    Any model field accepting `VariableType` in addition to its declared type,
    allows values to be specified as variable references or expressions in
    YAML configs and resolved at deploy time.

    The string **must** start with `${` — arbitrary strings are not accepted.
    Two syntaxes are supported:

    - Simple substitution: `${vars.my_variable_name}`
    - Python expression:   `${{ 4 if vars.env == 'prod' else 1 }}`

    Examples
    --------
    ```yaml
    num_workers: ${vars.workers}
    size: ${{ 4 if vars.env == 'prod' else 1 }}
    access_controls: ${vars.default_access_controls}
    ```

    References
    ----------
    * [variables](https://www.laktory.ai/concepts/variables/)
    """

    @classmethod
    def __get_pydantic_core_schema__(
        cls, source: type, handler: callable
    ) -> CoreSchema:
        return core_schema.str_schema(pattern=r"^\$\{")
