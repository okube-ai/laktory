from typing import Any

import typer

from laktory._version import VERSION

APP_NAME = "laktory-cli"


class LaktoryTyper(typer.Typer):
    """Typer app printing stack validation errors without a traceback."""

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        # Other validation errors (e.g. of a model built at runtime) are not
        # stack errors: they keep their traceback
        from laktory.cli._common import InvalidStackError

        try:
            return super().__call__(*args, **kwargs)
        except InvalidStackError as e:
            typer.secho(str(e), fg=typer.colors.RED, err=True)
            raise SystemExit(1) from None


app = LaktoryTyper(
    pretty_exceptions_show_locals=False,
    help=f"Laktory {VERSION} CLI to preview and deploy resources",
)  # prevent display secret data


@app.callback(invoke_without_command=True)
def main(
    ctx: typer.Context,
    version: bool = typer.Option(
        False, "--version", "-v", help="Show laktory CLI version"
    ),
):
    """ """
    if version:
        print(f"Laktory CLI version {VERSION}")
        raise typer.Exit()
