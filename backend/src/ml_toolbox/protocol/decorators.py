import inspect
from typing import Any

from ml_toolbox.protocol.types import PortType

NODE_REGISTRY: dict[str, dict[str, Any]] = {}


def node(
    inputs: dict[str, PortType] | None = None,
    outputs: dict[str, PortType] | None = None,
    params: dict[str, Any] | None = None,
    *,
    label: str | None = None,
    category: str | None = None,
    description: str | None = None,
):
    """Decorator that registers a node function into the global NODE_REGISTRY."""

    inputs = inputs or {}
    outputs = outputs or {}
    params = params or {}

    def decorator(fn):
        node_id = fn.__module__ + "." + fn.__name__

        # Derive category from module path: ml_toolbox.nodes.demo -> Demo
        module_parts = fn.__module__.split(".")
        auto_category = module_parts[-1].replace("_", " ").title() if len(module_parts) > 1 else "General"

        # Derive label from function name: clean_data -> Clean Data
        auto_label = fn.__name__.replace("_", " ").title()

        # Serialize params, injecting the name from the dict key
        serialized_params = []
        for param_name, param_obj in params.items():
            param_obj._name = param_name
            serialized_params.append(param_obj.serialize())

        NODE_REGISTRY[node_id] = {
            "type": node_id,
            "label": label or auto_label,
            "category": category or auto_category,
            "description": description or (inspect.getdoc(fn) or ""),
            "inputs": [{"name": k, "type": v.value} for k, v in inputs.items()],
            "outputs": [{"name": k, "type": v.value} for k, v in outputs.items()],
            "params": serialized_params,
            "default_code": inspect.getsource(fn),
        }

        return fn

    return decorator
