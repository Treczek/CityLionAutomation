import os

from src.parsers import MBankParser, BaselinkerParser
from src.utils.configure_logging import setup_logging


def run_mbank():
    setup_logging(3)
    MBankParser('Analityka finansowa').parse()


def run_baselinker():
    setup_logging(3)
    BaselinkerParser('Analityka finansowa', os.environ['BASELINKER_API']).parse()
