import typer

APP_NAME = "laktory-cli"

app = typer.Typer(
    pretty_exceptions_show_locals=False,
    help="Laktory CLI to preview and deploy resources",
)  # prevent display secret data


if __name__ == "__main__":
    app()
