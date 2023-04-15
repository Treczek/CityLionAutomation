import argparse
import os
from typing import Optional, Sequence
from src.parsers import MBankParser, BaselinkerParser
from src.scrapers.product_prices_and_availability.controller import PriceAndAvailabilityScraper
from src.utils.configure_logging import setup_logging


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description='Run one or more of the three functions: MBankParser, BaselinkerParser or PriceAndAvailabilityScraper.')

    parser.add_argument('-v', '--verbose', help='Verbose of logging module', default=3)
    parser.add_argument('--worksheet_name_parsers', default='Analityka finansowa',
                        help='Worksheet name to be processed for MBankParser and BaselinkerParser (default: "Analityka finansowa")')
    parser.add_argument('--worksheet_name_scraper', default='Nowe Arkusze kalkulacyjne ',
                        help='Worksheet name to be processed for PriceAndAvailabilityScraper (default: "Nowe Arkusze kalkulacyjne ")')
    parser.add_argument('-f', '--function', choices=['mbank', 'baselinker', 'scraper', 'all'], required=True,
                        help='Choose which function(s) to run: mbank, baselinker, scraper or all.')

    args = parser.parse_args(argv)
    setup_logging(args.verbose)

    if args.function == 'mbank':
        MBankParser(args.worksheet_name_parsers).parse()
    elif args.function == 'baselinker':
        BaselinkerParser(args.worksheet_name_parsers, os.environ['BASELINKER_API']).parse()
    elif args.function == 'scraper':
        PriceAndAvailabilityScraper(args.worksheet_name_scraper).scrape()
    elif args.function == 'all':
        MBankParser(args.worksheet_name_parsers).parse()
        BaselinkerParser(args.worksheet_name_parsers, os.environ['BASELINKER_API']).parse()
        PriceAndAvailabilityScraper(args.worksheet_name_scraper).scrape()
    else:
        print("Invalid function. Choose between 'mbank', 'baselinker', 'scraper', and 'all'.")

    return 0


if __name__ == "__main__":
    exit(main())
