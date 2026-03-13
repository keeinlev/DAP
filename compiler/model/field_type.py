from enum import StrEnum, auto, nonmember

class FieldType(StrEnum):
    STRING = auto()
    INT = auto()
    FLOAT = auto()
    BOOLEAN = auto()
    TIMESTAMP = auto()
    JSON = auto()
    
    ALIASES = nonmember({
        "integer": INT,
        "number": FLOAT,
        "bool": BOOLEAN,
    })

    @classmethod
    def parse(cls, value: str) -> "FieldType":
        value = value.lower()

        if value in cls.ALIASES:
            return cls.ALIASES[value]

        return cls(value)
