FROM python:3.13-slim AS base

# Install system dependencies for Pipeline CC (tmux for session management,
# curl/git for Claude CLI install, nodejs/npm for Claude Code CLI)
RUN apt-get update && apt-get install -y --no-install-recommends \
    tmux \
    curl \
    git \
    nodejs \
    npm \
    && rm -rf /var/lib/apt/lists/*

# Install Claude Code CLI
RUN npm install -g @anthropic-ai/claude-code

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

# Install dependencies first (cached layer)
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

# Copy source
COPY src/ src/
RUN uv sync --frozen --no-dev

EXPOSE 8000

CMD ["uv", "run", "uvicorn", "ml_toolbox.main:app", "--host", "0.0.0.0", "--port", "8000"]
