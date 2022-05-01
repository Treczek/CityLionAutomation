import argparse
import sys
from typing import Optional, Sequence
from src.parsers.mbank.mbank_parser import MBankParser

from src.utils.configure_logging import setup_logging

parser = argparse.ArgumentParser(description='Parser')
parser.add_argument('-v', '--verbose', help='Verbose of logging module', default=3)
parser.add_argument('spreadsheet_name', help='Spreadsheet name that needs to be parsed')


def main(argv: Optional[Sequence[str]] = None) -> int:
    setup_logging(args.verbose)
    MBankParser(args.spreadsheet_name).parse()
    return 0


if __name__ == "__main__":
    args = argparse.Namespace(spreadsheet_name='Analityka finansowa', verbose=1)
    exit(main(args))
