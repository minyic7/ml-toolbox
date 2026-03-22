# CLAUDE.md — ML-Toolbox

## CRITICAL: Sandbox Execution Rules

All node functions decorated with `@node()` run inside isolated Docker sandbox containers.
The sandbox runner extracts ONLY the function body and exec's it — **module-level code is invisible**.

### You MUST:
1. **Import ALL dependencies inside the function body** — `import pandas as pd`, `import json`, `import numpy as np`, etc. Module-level imports do NOT work.
2. **Define ALL helper functions inside the function body** — any function called from within a node function must be defined as a nested function inside it. Module-level helpers cause `NameError`.
3. **Never reference module-level variables** — constants, dicts, or any module-scope binding.

### Example — WRONG:
```python
import pandas as pd  # module-level — sandbox can't see this

def _helper():  # module-level — sandbox can't see this
    pass

@node(...)
def my_node(inputs, params):
    df = pd.read_parquet(inputs['df'])  # NameError: 'pd' not defined
    _helper()  # NameError: '_helper' not defined
```

### Example — CORRECT:
```python
@node(...)
def my_node(inputs, params):
    import pandas as pd  # inside function body ✅
    import json  # inside function body ✅

    def _helper():  # nested function ✅
        pass

    df = pd.read_parquet(inputs['df'])  # works ✅
    _helper()  # works ✅
```

This rule applies to ALL files in `backend/src/ml_toolbox/nodes/` — every `@node()` decorated function.
