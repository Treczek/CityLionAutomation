from abc import ABC, abstractmethod
import pandas as pd
from pathlib import Path


class Parser(ABC):

    def __init__(self, csv_file_path):
        self.csv_file_path = csv_file_path

    @abstractmethod
    def parse(self) -> pd.DataFrame:
        pass

