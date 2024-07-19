import typer
from laktory._version import VERSION

APP_NAME = "laktory-cli"

app = typer.Typer(
    pretty_exceptions_show_locals=False,
    help=f"Laktory {VERSION} CLI to preview and deploy resources",
)  # prevent display secret data


if __name__ == "__main__":
    app()
