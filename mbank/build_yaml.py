import pandas as pd
import yaml

rules = []
for rule_id, row in pd.read_csv("rules_swider.csv", header=None).filter([0, 1]).iterrows():
    rules.append(dict(id=rule_id, result_value=row[0], pattern=row[1]))

with open('mbank_mapping_rules.yml', 'w') as stream:
    yaml.dump(rules, stream)

print(f'{len(rules)} rules were built.')
