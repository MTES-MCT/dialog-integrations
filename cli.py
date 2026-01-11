import typer
from loguru import logger

app = typer.Typer()

@app.command()
def run():
    logger.info("Hello World")