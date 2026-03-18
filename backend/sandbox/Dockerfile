FROM python:3.13-slim

# Layer 1: heavy packages that rarely change (best cache hit rate)
RUN pip install --no-cache-dir numpy pyarrow polars

# Layer 2: packages that change occasionally
RUN pip install --no-cache-dir pandas scikit-learn

# Layer 3: runner script (changes most often — last for cache efficiency)
WORKDIR /sandbox
COPY runner.py /sandbox/runner.py
