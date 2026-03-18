from dataclasses import dataclass, field


@dataclass
class Select:
    options: list[str]
    default: str = ""
    _name: str = field(default="", init=False, repr=False)

    def serialize(self) -> dict:
        return {
            "type": "select",
            "name": self._name,
            "options": self.options,
            "default": self.default or self.options[0],
        }


@dataclass
class Slider:
    min: float
    max: float
    step: float = 1.0
    default: float = 0.0
    _name: str = field(default="", init=False, repr=False)

    def serialize(self) -> dict:
        return {
            "type": "slider",
            "name": self._name,
            "min": self.min,
            "max": self.max,
            "step": self.step,
            "default": self.default,
        }


@dataclass
class Text:
    default: str = ""
    _name: str = field(default="", init=False, repr=False)

    def serialize(self) -> dict:
        return {
            "type": "text",
            "name": self._name,
            "default": self.default,
        }


@dataclass
class Toggle:
    default: bool = False
    _name: str = field(default="", init=False, repr=False)

    def serialize(self) -> dict:
        return {
            "type": "toggle",
            "name": self._name,
            "default": self.default,
        }
