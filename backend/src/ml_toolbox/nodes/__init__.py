"""Auto-discover and import all node modules to trigger @node registration."""
import importlib
import pkgutil

# Import every .py module in this package so that @node decorators
# fire and populate NODE_REGISTRY automatically.
for _finder, _name, _ispkg in pkgutil.walk_packages(__path__, prefix=__name__ + "."):
    if not _ispkg:
        importlib.import_module(_name)
