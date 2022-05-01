import operator
from abc import ABC, abstractmethod
from functools import reduce
from typing import List, Optional, Union

import numpy as np
import pandas as pd
from pydantic import BaseModel

from src.exceptions import MaskCreationException


class MappingField(ABC, BaseModel):

    name: str
    value: Union[str, int]

    @abstractmethod
    def create_mask(self, df: pd.DataFrame) -> pd.Series:
        raise NotImplementedError

    def __repr__(self):
        return self.value


class PatternMappingField(MappingField):
    name: str
    value: str

    def create_mask(self, df: pd.DataFrame) -> pd.Series:
        return (
            df[self.name]
            .map(str)
            .str.lower()
            .str.contains(self.value.lower(), regex=False)
        )


class NumericalMappingField(MappingField):
    name: str
    value: Union[str, float]

    def create_mask(self, df: pd.DataFrame) -> pd.Series:

        operator_signs = {
            ">": operator.gt,
            "<": operator.lt,
            ">=": operator.ge,
            "<=": operator.le,
            "!=": operator.ne,
        }

        if type(self.value) == str and any(char in self.value for char in "!<>="):
            sign, comparison_value = self.value.split(" ")
            if sign not in operator_signs:
                raise MaskCreationException(
                    f"Operator sign {sign} cannot be used. Possible values: {list(operator_signs.keys())}"
                )
            return operator_signs[sign](df[self.name], float(comparison_value))

        return df[self.name] == float(self.value)


class MappingRule(BaseModel):
    id: int
    result_value: str
    description: Optional[PatternMappingField]
    category: Optional[PatternMappingField]
    mbank_category: Optional[PatternMappingField]
    year: Optional[NumericalMappingField]
    month: Optional[NumericalMappingField]
    type: Optional[PatternMappingField]
    EUR: Optional[NumericalMappingField]
    PLN: Optional[NumericalMappingField]
    currency: Optional[PatternMappingField]

    def create_mask(self, df) -> pd.Series:
        filters = [
            attr
            for attr in self.__dict__
            if attr not in ["id", "result_value"] and getattr(self, attr)
        ]
        masks = [getattr(self, attr).create_mask(df) for attr in filters]
        return reduce(
            np.logical_and, masks, pd.Series([True for _ in range(df.shape[0])])
        )

    def __str__(self):
        not_none_params = {k: v for k, v in self.__dict__.items() if v is not None}
        return f"{type(self).__name__} - {not_none_params}"


class MappingRules(BaseModel):
    mapping_rules: List[MappingRule]
