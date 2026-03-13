from dataclasses import dataclass
from .field_type import FieldType

@dataclass
class Field:
    name: str
    type: FieldType
    required: bool
