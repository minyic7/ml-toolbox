from enum import Enum


class PortType(str, Enum):
    TABLE = "TABLE"
    MODEL = "MODEL"
    METRICS = "METRICS"
    ARRAY = "ARRAY"
    VALUE = "VALUE"
    TENSOR = "TENSOR"


PORT_COLORS: dict[PortType, str] = {
    PortType.TABLE: "#9CA3AF",
    PortType.MODEL: "#22C55E",
    PortType.METRICS: "#EAB308",
    PortType.ARRAY: "#3B82F6",
    PortType.VALUE: "#A855F7",
    PortType.TENSOR: "#F97316",
}
