from pydantic import BaseModel


class AppConfig(BaseModel):
    debug: bool
    verbosity: int
