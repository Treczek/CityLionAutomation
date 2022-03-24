from src.parsers.mbank.mbank_parser import MBankParser
from src.parsers.mbank.mapping_rules import MappingRules

import yaml
from pathlib import Path

if __name__ == '__main__':
    from src.utils.configure_logging import setup_logging

    setup_logging(3)
    mbank_path = Path(
        '/Users/tomasz.reczek/Projekty/CityLionParser/templates/lista_operacji_210228_220228_202202282035279836.csv'
    )

    parser = MBankParser(csv_file_path=mbank_path)
    parser.parse()
