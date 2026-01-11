import typer
from loguru import logger

from settings import Settings

app = typer.Typer()


@app.command()
def run():
    logger.info("Hello World")
    settings = Settings()  # type: ignore[call-arg]
    logger.info(f"Base URL: {settings.base_url}")
