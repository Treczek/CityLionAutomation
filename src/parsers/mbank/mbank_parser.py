from pathlib import Path

import pandas as pd
import numpy as np
import re

from src.parsers.base import Parser
from src.parsers.mbank.mapping_rules import MappingRules, MappingRule
from src.api.nbp_api import NBPApi


class MBankParser(Parser):

    def __init__(self, csv_file_path: Path, mapping_rules: MappingRules):
        super().__init__(csv_file_path)
        self.mapping_rules = mapping_rules
        self.nbp_api = NBPApi()

    def parse(self) -> pd.DataFrame:
        df = self.load_dataframe()
        df = self.data_preparation(df)
        for mapping_rule in self.mapping_rules.mapping_rules:
            df = self.fill_categories(df, mapping_rule)
        df['rules_triggered'] = df['rules_triggered'].str.strip()
        return df

    def fill_categories(self, df: pd.DataFrame, mapping_rule: MappingRule) -> pd.DataFrame:

        mask = df['description'].str.match(mapping_rule.pattern, flags=re.IGNORECASE)

        df.loc[mask, 'category'] = mapping_rule.result_value
        df.loc[mask, 'rules_triggered'] += f" {mapping_rule.id}"

        return df

    def load_dataframe(self) -> pd.DataFrame:
        return pd.read_csv(self.csv_file_path, sep=';', encoding='windows-1250', skiprows=27)

    def data_preparation(self, df: pd.DataFrame) -> pd.DataFrame:
        def parse_amount(series):
            return (
                series
                    .str
                    .strip(' EURPLN')
                    .replace("\s", "", regex=True)
                    .replace(",", ".", regex=True)
                    .map(float)
            )

        mapping = {
            'index': 'date',
            '#Data operacji': 'description',
            '#Opis operacji': 'account',
            '#Rachunek': 'mbank_category',
            '#Kategoria': 'amount'
        }

        df = df.reset_index()
        df.columns = [mapping.get(col) for col in df.columns]
        df = df.dropna(axis=1)

        return (
            df
            .assign(
                currency=df['amount'].str[-3:],
                date=pd.to_datetime(df['date']),
                amount=parse_amount(df['amount']),
                rules_triggered='')
            .assign(
                type=lambda df: np.where(df['amount'] > 0, 'wpływ', 'wydatek')
            )
            # .pipe(self.calculate_currencies)
            .drop(columns=['account', 'mbank_category'])
        )

    def calculate_currencies(self, df):
        date_min, date_max = df.date.min(), df.date.max()
        all_dates = pd.DataFrame(pd.date_range(date_min, date_max)).rename(columns={0: "all_dates"})

        # TODO prototypes.ipynb już się liczy
        rates = pd.merge_asof(
            left=all_dates,
            right=self.nbp_api.get_rates(date_min, date_max),
            left_on='all_dates',
            right_on='date'
        ).fillna(method='backfill')

        return df