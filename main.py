import argparse
import os
from typing import Optional, Sequence
from src.parsers import MBankParser, BaselinkerParser

from src.utils.configure_logging import setup_logging

parser = argparse.ArgumentParser(description='Parser')
parser.add_argument('-v', '--verbose', help='Verbose of logging module', default=3)
parser.add_argument('spreadsheet_name', help='Spreadsheet name that needs to be parsed')


def main(argv: Optional[Sequence[str]] = None) -> int:
    setup_logging(args.verbose)
    MBankParser(args.spreadsheet_name).parse()
    BaselinkerParser(args.spreadsheet_name, os.environ['BASELINKER_API']).parse()
    return 0


if __name__ == "__main__":
    # TODO Sortowanie mapy baselinkera
    # TODO Upewnienie sie ze nie kasujemy juz zmapowanych produktow po usunieciu archiwum
    args = argparse.Namespace(spreadsheet_name='Analityka finansowa', verbose=2)
    # args = argparse.Namespace(spreadsheet_name='TiA finanse', verbose=2)
    exit(main(args))
