# https://www.youtube.com/watch?v=bu5wXjz2KvU

import gspread
import pandas as pd
import structlog


class GSheetConnection:
    def __init__(self, file_name: str, create_if_missing: bool = False):

        # Authorization requires service_account.json with credentials located in ~/.config/gspread directory
        self.logger = structlog.getLogger(__name__)

        self.gc = gspread.service_account()
        self.spreadsheet = self._get_spreadsheet(file_name, create_if_missing)

    def __getitem__(self, sheet_name):
        if type(sheet_name) == str:
            sheet = self._get_worksheet(sheet_name, False)
        else:
            raise NotImplemented(f"{type(sheet_name)} is not implemented at the moment")
        self.logger.debug("Worksheet found and hooked on.", worksheet=sheet)
        return GWorksheet(sheet)

    def new_worksheet(self, sheet_name):
        return self._get_spreadsheet(sheet_name, True)

    def _get_spreadsheet(
        self, file_name: str, create_if_missing: bool
    ) -> gspread.Spreadsheet:
        try:
            spreadsheet = self.gc.open(file_name)
            self.logger.info("Spreadsheet opened", file_name=file_name)
        except gspread.exceptions.SpreadsheetNotFound:
            if create_if_missing:
                spreadsheet = self.gc.create(file_name)
                self.logger.info("Spreadsheet created", file_name=file_name)
            else:
                self.logger.error("Spreadsheet does not exist", file_name=file_name)
                raise

        return spreadsheet

    def _get_worksheet(
        self, sheet_name: str, create_if_missing: bool
    ) -> gspread.Worksheet:
        try:
            sheet = self.spreadsheet.worksheet(sheet_name)
            self.logger.info(
                "Sheet hook created", file_name=self.spreadsheet.title, sheet_name=sheet
            )
        except gspread.exceptions.WorksheetNotFound:
            if create_if_missing:
                sheet = self.spreadsheet.add_worksheet(
                    title=sheet_name, rows=1000, cols=20
                )
                self.logger.info(
                    "Sheet created and hooked",
                    file_name=self.spreadsheet.title,
                    sheet_name=sheet_name,
                )
            else:
                self.logger.error(
                    "Worksheet does not exist",
                    file_name=self.spreadsheet.title,
                    sheet_name=sheet_name,
                )
                raise

        return sheet

    @property
    def worksheets(self):
        return [sh.title for sh in self.gc.openall()]


class GWorksheet:
    def __init__(self, worksheet):
        self.logger = structlog.getLogger(__name__)
        self.worksheet = worksheet

    def get_data(self) -> pd.DataFrame:
        return pd.DataFrame(self.worksheet.get_all_records())

    def update_data(self, data):
        self.worksheet.update([data.columns.values.tolist()] + data.values.tolist())
