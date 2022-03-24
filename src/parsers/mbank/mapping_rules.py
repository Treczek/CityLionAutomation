from pydantic import BaseModel
from typing import List, Optional


class MappingRule(BaseModel):
    id: int
    result_value: str
    pattern: Optional[str]


class MappingRules(BaseModel):
    mapping_rules: List[MappingRule]
