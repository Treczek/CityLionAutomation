from src.parsers.mbank.mbank_parser import MBankParser
from pathlib import Path
import click

from src.utils.configure_logging import setup_logging


@click.group()
def run():
    pass


@run.command()
@click.argument("name")
@click.option("--verbose", default=3)
def mbank_parse(**kwargs):
    setup_logging(kwargs["verbose"])
    MBankParser(spreadsheet_name=kwargs["name"]).parse()
    return 1


if __name__ == "__main__":
    from click.testing import CliRunner
    runner = CliRunner()
    runner.invoke(mbank_parse, ['Analityka finansowa'])
