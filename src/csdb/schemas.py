from pydantic import BaseModel, Field


class CSRS_Model(BaseModel):
    def __str__(self) -> str:
        s = self.__class__.__name__
        attrs = list()
        for name, field in self.__class__.model_fields.items():
            if field.repr:
                attrs.append(f"{name}={getattr(self, name)}")
        if attrs:
            attrs = ", ".join(attrs)

        return f"{s}({attrs})"


class Run(CSRS_Model):
    """Represents a single run of CalSim

    Uses pydantic to do data validation."""

    name: str
    """The human readable name for the Run"""
    source: str
    """The source of the data for the Run"""


class Variable(CSRS_Model):
    """Represents a State Variable or Decision Variable in CalSim3

    Uses pydantic to do data validation."""

    name: str
    """The human readable name for the Variable"""
    code_name: str
    """The name used in WRIMS for the Variable"""
    kind: str = Field(repr=False)
    """The kind used in WRIMS for the Variable"""
    units: str = Field(repr=False)
    """The units used in WRIMS for the Variable"""
