from pydantic import BaseModel
from typing import List
import yaml


class MappingRule(BaseModel):
    id: int
    result_value: str
    pattern: str


class MappingRules(BaseModel):
    mapping_rules: List[MappingRule]


if __name__ == '__main__':
    path = '/Users/tomasz.reczek/Projekty/CityLionParser/mbank/mbank_mapping_rules.yml'
    with open(path, 'r') as stream:
        rules = MappingRules(**yaml.safe_load(stream))
    print(rules)
