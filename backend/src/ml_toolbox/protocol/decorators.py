import inspect
from typing import Any

from ml_toolbox.protocol.types import PortType

NODE_REGISTRY: dict[str, dict[str, Any]] = {}


def node(
    inputs: dict[str, PortType] | None = None,
    outputs: dict[str, PortType] | None = None,
    params: dict[str, Any] | None = None,
):
    """Decorator that registers a node function into the global NODE_REGISTRY."""

    inputs = inputs or {}
    outputs = outputs or {}
    params = params or {}

    def decorator(fn):
        node_id = fn.__module__ + "." + fn.__name__

        # Serialize params, injecting the name from the dict key
        serialized_params = []
        for param_name, param_obj in params.items():
            param_obj._name = param_name
            serialized_params.append(param_obj.serialize())

        NODE_REGISTRY[node_id] = {
            "id": node_id,
            "name": fn.__name__,
            "inputs": [{"name": k, "type": v.value} for k, v in inputs.items()],
            "outputs": [{"name": k, "type": v.value} for k, v in outputs.items()],
            "params": serialized_params,
            "code": inspect.getsource(fn),
        }

        return fn

    return decorator
