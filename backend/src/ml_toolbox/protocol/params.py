from dataclasses import dataclass, field


@dataclass
class Select:
    options: list[str]
    default: str = ""
    description: str = ""
    _name: str = field(default="", init=False, repr=False)

    def serialize(self) -> dict:
        d: dict = {
            "type": "select",
            "name": self._name,
            "options": self.options,
            "default": self.default or self.options[0],
        }
        if self.description:
            d["description"] = self.description
        return d


@dataclass
class Slider:
    min: float
    max: float
    step: float = 1.0
    default: float = 0.0
    description: str = ""
    _name: str = field(default="", init=False, repr=False)

    def serialize(self) -> dict:
        d: dict = {
            "type": "slider",
            "name": self._name,
            "min": self.min,
            "max": self.max,
            "step": self.step,
            "default": self.default,
        }
        if self.description:
            d["description"] = self.description
        return d


@dataclass
class Text:
    default: str = ""
    description: str = ""
    placeholder: str = ""
    _name: str = field(default="", init=False, repr=False)

    def serialize(self) -> dict:
        d: dict = {
            "type": "text",
            "name": self._name,
            "default": self.default,
        }
        if self.description:
            d["description"] = self.description
        if self.placeholder:
            d["placeholder"] = self.placeholder
        return d


@dataclass
class Toggle:
    default: bool = False
    description: str = ""
    _name: str = field(default="", init=False, repr=False)

    def serialize(self) -> dict:
        d: dict = {
            "type": "toggle",
            "name": self._name,
            "default": self.default,
        }
        if self.description:
            d["description"] = self.description
        return d
