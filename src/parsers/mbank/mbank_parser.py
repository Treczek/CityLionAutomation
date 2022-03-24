from pathlib import Path

import pandas as pd
import numpy as np
import structlog

from src.parsers.mbank.mapping_rules import MappingRule, MappingRules
from src.api.nbp_api import NBPApi
from src.gdrive_connection.base import GSheetConnection
from src.utils.steps import apply_steps
from src.utils.gsheet_types import datetime_to_excel_date

# TODO dodac walidacje na zdublowane reguly
# TODO dodac grupowanie niezmapowanych po przychodzie
# TODO poprawienie indeksowania reguly


class MBankParser:

    def __init__(self, csv_file_path: Path):
        self.logger = structlog.getLogger(__name__)
        self.csv_file_path = csv_file_path

        self.nbp_api = NBPApi()
        self.spreadsheet = GSheetConnection('Analityka finansowa')

    def parse(self):

        steps = [
            (self.load_bank_billing, {}),
            (self.data_preparation, {}),
            (self.calculate_currencies, {}),
            (self.assign_initial_categories, {}),
            (self.assign_manual_categories, {}),
            (self.format_before_pushing, {}),
            (self.check_double_entries, {}),
            (self.push_processed_data, {}),
            (self.format_after_pushing, {})
        ]

        apply_steps(steps, logger=self.logger)

    def load_bank_billing(self, dummy=None) -> pd.DataFrame:
        # TODO wczytywanie ze spreadsheet
        return pd.read_csv(self.csv_file_path, sep=';', encoding='windows-1250', skiprows=27)

    def data_preparation(self, df: pd.DataFrame) -> pd.DataFrame:

        # TODO naprawic debugger
        def parse_amount(value):
            return (
                float(
                    value
                    .strip(' PLNEUR')
                    .replace(" ", "")
                    .replace(",", ".")
                )
            )

        mapping = {
            'index': 'date',
            '#Data operacji': 'description',
            '#Opis operacji': 'account',
            '#Rachunek': 'mbank_category',
            '#Kategoria': 'amount',
            'level_0': 'id'
        }

        df = df.reset_index().reset_index()
        df.columns = [mapping.get(col) for col in df.columns]
        df = df.dropna(axis=1)

        return (
            df
            .assign(currency=df['amount'].str[-3:],
                    date=pd.to_datetime(df['date']),
                    amount=df['amount'].apply(parse_amount),
                    type=lambda df: np.where(df['amount'] > 0, 'Wpływ', 'Wydatek'),
                    rules_triggered="")
            .drop(columns=['account', 'mbank_category'])
        )

    def calculate_currencies(self, df) -> pd.DataFrame:
        date_range = pd.date_range(df.date.min(), df.date.max())
        all_dates = pd.DataFrame(date_range).rename(columns={0: "all_dates"})

        rates = pd.merge_asof(
            left=all_dates,
            right=self.nbp_api.get_rates(date_range),
            left_on='all_dates',
            right_on='date'
        ).fillna(method='backfill')

        df = df.merge(
            rates.drop('date', axis=1).rename(columns={'all_dates': 'date'}),
            on='date',
            how='left'
        )

        df.loc[df['currency'] == 'PLN', 'EUR'] = round(df['amount'] / df['rate'], 2)
        df['EUR'].fillna(df['amount'], inplace=True)

        df.loc[df['currency'] == 'EUR', 'PLN'] = round(df['amount'] * df['rate'], 2)
        df['PLN'].fillna(df['amount'], inplace=True)

        df = df.drop(columns=['amount', 'rate'])

        return df

    def assign_initial_categories(self, df: pd.DataFrame) -> pd.DataFrame:
        def fill_categories(df: pd.DataFrame, mapping_rule: MappingRule) -> pd.DataFrame:
            mask = df['description'].map(str).str.lower().str.contains(mapping_rule.pattern.lower(), regex=False)
            if mask.sum() == 0:
                self.logger.warning('Rule did not match any record', pattern=mapping_rule.pattern)
                return df
            df.loc[mask, 'category'] = mapping_rule.result_value
            df.loc[mask, 'rules_triggered'] += f" {mapping_rule.id}"
            return df

        rules = self.spreadsheet['ParserPatternRules'].get_data()
        mapping_rules = MappingRules(
            mapping_rules=pd.DataFrame(rules).assign(id=lambda df: df['id'].map(int))[['id', 'pattern', 'result_value']].to_dict('records')
        )
        self.logger.info(f"Fetched {len(mapping_rules.mapping_rules)} pattern rules")

        for mapping_rule in mapping_rules.mapping_rules:
            df = fill_categories(df, mapping_rule)
        df['rules_triggered'] = df['rules_triggered'].str.strip()

        return df

    def assign_manual_categories(self, df: pd.DataFrame) -> pd.DataFrame:
        # TODO DRY
        # TODO walidacja czy id jest unikalne
        def fill_categories(df: pd.DataFrame, mapping_rule: MappingRule) -> pd.DataFrame:

            mask = df['id'].map(int) == mapping_rule.id
            if mask.sum() == 0:
                self.logger.warning('Index rule did not match any record', pattern=mapping_rule.id)
                return df
            df.loc[mask, 'category'] = mapping_rule.result_value
            df.loc[mask, 'rules_triggered'] = f"Index rule {mapping_rule.id} "
            return df

        rules = self.spreadsheet['ParserIndexRules'].get_data()
        mapping_rules = pd.DataFrame(rules).assign(id=lambda df: df['id'].map(int))[['id', 'result_value']].to_dict('records')
        mapping_rules = MappingRules(
            mapping_rules=mapping_rules
        )
        for mapping_rule in mapping_rules.mapping_rules:
            df = fill_categories(df, mapping_rule)
        df['rules_triggered'] = df['rules_triggered'].str.strip()

        return df

    def format_before_pushing(self, df: pd.DataFrame) -> pd.DataFrame:
        df['year'] = df['date'].dt.year
        df['month'] = df['date'].dt.month
        df['date'] = df['date'].apply(datetime_to_excel_date)
        df['category'] = df['category'].fillna("Not mapped")
        df[['EUR', 'PLN']] = df[['EUR', 'PLN']].applymap(float)

        return df[['id', 'date', 'year', 'month', 'description', 'type', 'category', 'rules_triggered', 'EUR', 'PLN', 'currency']]

    def check_double_entries(self, df: pd.DataFrame) -> pd.DataFrame:

        pattern_rules = self.spreadsheet['ParserPatternRules'].get_data()

        mask = df['rules_triggered'].str.split(" ").map(len) >= 2
        duplicated_rules = df.loc[mask, 'rules_triggered'].drop_duplicates()
        for id, rules in duplicated_rules.iteritems():
            if 'Index' in rules:
                continue
            pattern_mask = pattern_rules.id.isin(map(int, rules.split(" ")))
            if pattern_rules.loc[pattern_mask, 'result_value'].nunique() > 1:
                patterns = pattern_rules.loc[pattern_mask, 'pattern'].unique()
                result_values = pattern_rules.loc[pattern_mask, 'result_value'].unique()
                self.logger.warning("More then one rule mapped to transaction",
                                    id=id, rules=rules, patterns=list(zip(patterns, result_values)))

        return df

    def push_processed_data(self, df: pd.DataFrame):
        ws = self.spreadsheet['ParsedData']
        ws.worksheet.clear()
        ws.update_data(df)

    def format_after_pushing(self, dummy=None):

        # Date with proper format
        requests = [{
            "repeatCell": {
                "range": {
                    "startColumnIndex": 1,
                    "endColumnIndex": 2,
                    "sheetId": self.spreadsheet['ParsedData'].worksheet._properties['sheetId']
                },
                "cell": {
                    "userEnteredFormat": {
                        "numberFormat": {
                            "type": "DATE",
                            "pattern": "yyyy-mm-dd"
                        }
                    }
                },
                "fields": "userEnteredFormat.numberFormat"
            }
        }]

        self.spreadsheet.spreadsheet.batch_update({'requests': requests})
