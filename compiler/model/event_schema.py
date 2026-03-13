from dataclasses import dataclass
from .field import Field

@dataclass
class EventSchema:
    name: str
    version: int
    fields: list[Field]
