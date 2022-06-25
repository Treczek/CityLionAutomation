import numpy as np
import pandas as pd
import structlog

from src.api.nbp_api import NBPApi
from src.gdrive_connection.base import GSheetConnection
from src.parsers.mbank.mapping_rules import MappingRule, MappingRules
from src.utils.gsheet_types import datetime_to_excel_date
from src.utils.steps import apply_steps


class MBankParser:
    def __init__(self, spreadsheet_name: str):
        self.logger = structlog.getLogger(__name__)

        self.nbp_api = NBPApi()
        self.spreadsheet = GSheetConnection(spreadsheet_name)

        self.warnings = []

    def _warn_with_caching(self, message):
        self.warnings.append(message)
        self.logger.warning(message)

    def parse(self):

        steps = [
            (self.load_bank_billings, {}),
            (self.data_preparation, {}),
            (self.add_manual_entries, {}),
            (self.calculate_currencies, {}),
            (self.assign_initial_categories, {}),
            (self.assign_manual_categories, {}),
            (self.add_upper_categories, {}),
            (self.format_before_pushing, {}),
            (self.check_double_entries, {}),
            (self.save_not_mapped_records, {}),
            (self.push_processed_data, {}),
            (self.format_after_pushing, {}),
            (self.push_warnings, {})
        ]

        apply_steps(steps, logger=self.logger)

    def load_bank_billings(self, dummy=None) -> pd.DataFrame:
        billing_sheets = [
            sheet for sheet in self.spreadsheet.spreadsheet.worksheets() if "MbankBilling" in sheet.title
        ]

        if not billing_sheets:
            error_msg = 'There is no single worksheet with "MbankBilling" in title'
            self.logger.error(error_msg)
            raise ValueError(error_msg)

        return pd.concat([self._load_bank_billing(sheet.title) for sheet in billing_sheets], axis=0)

    def _load_bank_billing(self, worksheet_name) -> pd.DataFrame:

        billing = self.spreadsheet[worksheet_name].worksheet.get_all_values()
        for to_skip, line in enumerate(billing):
            if line[0] == "#Data operacji":
                break

        billing = billing[to_skip:]
        return pd.DataFrame(data=billing[1:], columns=billing[0])

    def data_preparation(self, df: pd.DataFrame) -> pd.DataFrame:
        def parse_amount(value):
            return float(value.strip(" PLNEUR").replace(" ", "").replace(",", "."))

        mapping = {
            "#Data operacji": "date",
            "#Opis operacji": "description",
            "#Rachunek": "account",
            "#Kategoria": "mbank_category",
            "#Kwota": "amount",
            "level_0": "id",
        }

        df.columns = [mapping.get(col) for col in df.columns]
        df = df.dropna(axis=1)
        df = df.assign(
            currency=df["amount"].str[-3:],
            date=pd.to_datetime(df["date"]),
            amount=df["amount"].apply(parse_amount),
            type=lambda df: np.where(df["amount"] > 0, "Wpływ", "Wydatek"),
            category="Not mapped",
            rules_triggered="",
        ).sort_values(["date", "description", "amount"], ascending=True)

        return df.assign(id=range(df.shape[0]))

    def add_manual_entries(self, df: pd.DataFrame) -> pd.DataFrame:

        manual_entries = self.spreadsheet["ManualEntries"].get_data()
        if manual_entries.shape == (0, 0):
            return df

        manual_entries = manual_entries.assign(
            date=pd.to_datetime(manual_entries["date"]),
            mbank_category="Manual entry",
            type=np.where(manual_entries["amount"] > 0, "Wpływ", "Wydatek"),
            rules_triggered="",
            id=range(df.shape[0], df.shape[0] + manual_entries.shape[0]),
        )

        return pd.concat([df, manual_entries])

    def calculate_currencies(self, df) -> pd.DataFrame:
        date_range = pd.date_range(
            df.date.min(), min(df.date.max(), pd.Timestamp.today())
        )
        all_dates = pd.DataFrame(date_range).rename(columns={0: "all_dates"})

        rates = pd.merge_asof(
            left=all_dates,
            right=self.nbp_api.get_rates(date_range),
            left_on="all_dates",
            right_on="date",
        ).fillna(method="backfill")

        df = df.merge(
            rates.drop("date", axis=1).rename(columns={"all_dates": "date"}),
            on="date",
            how="left",
        )

        df.loc[df["currency"] == "PLN", "EUR"] = round(df["amount"] / df["rate"], 2)
        df["EUR"].fillna(df["amount"], inplace=True)

        df.loc[df["currency"] == "EUR", "PLN"] = round(df["amount"] * df["rate"], 2)
        df["PLN"].fillna(df["amount"], inplace=True)

        df = df.drop(columns=["amount", "rate"])

        return df

    def assign_initial_categories(self, df: pd.DataFrame) -> pd.DataFrame:

        mappable = df["mbank_category"] != "Manual entry"

        def fill_categories(
            df: pd.DataFrame, mapping_rule: MappingRule
        ) -> pd.DataFrame:

            mask = mapping_rule.create_mask(df) & mappable

            if mask.sum() == 0:

                self._warn_with_caching(
                    f"Rule did not match any record. Rule: {str(mapping_rule)}."
                )
                return df
            df.loc[mask, "category"] = mapping_rule.result_value
            df.loc[mask, "rules_triggered"] += f" {mapping_rule.id}"
            return df

        def transform_row_into_mapping_rule(dct: dict) -> dict:
            for key in list(dct.keys())[::-1]:
                if key not in ["id", "result_value"]:
                    if dct[key]:
                        dct[key] = {"name": key, "value": dct[key]}
                    else:
                        del dct[key]
            return dct

        rules = self.spreadsheet["PatternRules"].get_data()

        # Updating ids
        rules["id"] = pd.Series(rules.index).map(int)
        self.spreadsheet["PatternRules"].update_data(rules)

        mapping_rules = [
            transform_row_into_mapping_rule(dct)
            for dct in pd.DataFrame(rules).to_dict("records")
        ]
        mapping_rules = MappingRules(mapping_rules=mapping_rules)
        self.logger.info(f"Fetched {len(mapping_rules.mapping_rules)} mapping rules")

        for mapping_rule in mapping_rules.mapping_rules:
            df = fill_categories(df, mapping_rule)
        df["rules_triggered"] = df["rules_triggered"].str.strip()

        return df

    def assign_manual_categories(self, df: pd.DataFrame) -> pd.DataFrame:
        # TODO DRY
        # TODO Warn about mapping the last day
        def fill_categories(
            df: pd.DataFrame, mapping_rule: MappingRule
        ) -> pd.DataFrame:

            mask = df["id"].map(int) == mapping_rule.id
            if mask.sum() == 0:
                self._warn_with_caching(
                    f"Index rule did not match any record. Rule ID: {mapping_rule.id}"
                )
                return df
            df.loc[mask, "category"] = mapping_rule.result_value
            df.loc[mask, "rules_triggered"] = f"Index rule {mapping_rule.id} "
            return df

        rules = self.spreadsheet["IndexRules"].get_data()
        rules = rules[['id', 'result_value']].query("id != ''").dropna()
        if rules.id.duplicated().sum() > 0:
            duplicated_ids = rules.loc[rules.id.duplicated(), "id"].values.tolist()
            self._warn_with_caching(
                f"Your `IndexRules` worksheet contains duplicated ids - parser used only the last one of each. \
Duplicated IDs: {', '.join(map(str, duplicated_ids))}"
            )

        mapping_rules = (
            pd.DataFrame(rules)
            .assign(id=lambda df: df["id"].map(int))[["id", "result_value"]]
            .to_dict("records")
        )
        mapping_rules = MappingRules(mapping_rules=mapping_rules)
        for mapping_rule in mapping_rules.mapping_rules:
            df = fill_categories(df, mapping_rule)
        df["rules_triggered"] = df["rules_triggered"].str.strip()

        return df

    def format_before_pushing(self, df: pd.DataFrame) -> pd.DataFrame:
        df["year"] = df["date"].dt.year
        df["month"] = df["date"].dt.month
        df['year-month'] = df['date'].dt.strftime("%Y-%m")
        df["date"] = df["date"].apply(datetime_to_excel_date)
        df["category"] = df["category"].fillna("Not mapped")
        df[["EUR", "PLN"]] = df[["EUR", "PLN"]].applymap(float)
        df["PLN abs"] = abs(df["PLN"])

        detailed_categories = [col for col in df.columns if 'level' in col]

        return df[
            [
                "id",
                "date",
                "year",
                "month",
                'year-month',
                "description",
                "type",
                "category",
                *detailed_categories,
                "mbank_category",
                "rules_triggered",
                "EUR",
                "PLN",
                "PLN abs",
                "currency",
            ]
        ]

    def check_double_entries(self, df: pd.DataFrame) -> pd.DataFrame:

        pattern_rules = self.spreadsheet["PatternRules"].get_data()

        mask = df["rules_triggered"].str.split(" ").map(len) >= 2
        duplicated_rules = df.loc[mask, "rules_triggered"].drop_duplicates()
        for id, rules in duplicated_rules.iteritems():
            if "Index" in rules:
                continue
            # TODO do przepisania po implementacji wszystkich filtrow
            pattern_mask = pattern_rules.id.isin(map(int, rules.split(" ")))
            if pattern_rules.loc[pattern_mask, "result_value"].nunique() > 1:
                self._warn_with_caching(
                    f"More then one rule mapped to the transaction. Transaction id {id}. Rule ids {rules}."
                )

        return df

    def save_not_mapped_records(self, df: pd.DataFrame) -> pd.DataFrame:

        not_mapped_worksheet = self.spreadsheet["NotMapped"]

        not_mapped = (
            df.query('category == "Not mapped"')
            .assign(abs_value=lambda df: abs(df["PLN"]))
            .sort_values("abs_value", ascending=False)
        )

        not_mapped_worksheet.worksheet.clear()
        not_mapped_worksheet.update_data(not_mapped)

        return df

    def add_upper_categories(self, df: pd.DataFrame) -> pd.DataFrame:

        mapping = self.spreadsheet["CategoryMapping"].get_data()
        for column in mapping.columns:
            if 'level' in column:
                current_mapping = mapping[['Detailed', column]].set_index('Detailed').to_dict()[column]
                df[column] = df['category'].map(current_mapping).fillna('Without mapping')
        return df

    def push_processed_data(self, df: pd.DataFrame):
        ws = self.spreadsheet["ParsedData"]
        ws.worksheet.clear()
        ws.update_data(df)

    def format_after_pushing(self, dummy=None):

        # Date with proper format
        requests = [
            {
                "repeatCell": {
                    "range": {
                        "startColumnIndex": 1,
                        "endColumnIndex": 2,
                        "sheetId": self.spreadsheet["ParsedData"].worksheet._properties[
                            "sheetId"
                        ],
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "numberFormat": {"type": "DATE", "pattern": "yyyy-mm-dd"}
                        }
                    },
                    "fields": "userEnteredFormat.numberFormat",
                }
            }
        ]

        self.spreadsheet.spreadsheet.batch_update({"requests": requests})

    def push_warnings(self, dummy=None):
        logging_worksheet = self.spreadsheet['Warnings']
        logging_worksheet.worksheet.clear()
        logging_worksheet.update_data(pd.DataFrame(self.warnings, columns=['Warnings']))
