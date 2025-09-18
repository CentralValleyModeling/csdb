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
    """Represents a single run of CalSim"""

    name: str
    source: str


class Variable(CSRS_Model):
    """Represents a State Variable or Decision Variable in CalSim3"""

    name: str
    code_name: str
    kind: str = Field(repr=False)
    units: str = Field(repr=False)
