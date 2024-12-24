import typer
from laktory._version import VERSION

APP_NAME = "laktory-cli"

app = typer.Typer(
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


if __name__ == "__main__":
    app()
