from src.parsers.mbank.mbank_parser import MBankParser
from src.parsers.mbank.mapping_rules import MappingRules

import yaml
from pathlib import Path

if __name__ == '__main__':
    rules_path = Path('/Users/tomasz.reczek/Projekty/CityLionParser/mbank/mbank_mapping_rules.yml')
    mbank_path = Path(
        '/Users/tomasz.reczek/Projekty/CityLionParser/templates/lista_operacji_210228_220228_202202282035279836.csv'
    )

    with open(rules_path, 'r') as stream:
        rules = MappingRules(mapping_rules=yaml.safe_load(stream))

    parser = MBankParser(csv_file_path=mbank_path, mapping_rules=rules)
    data = parser.parse()
    data.to_csv("parsed_v1.csv")

