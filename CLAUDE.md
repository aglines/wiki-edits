# CLAUDE.md

## Python Environment

This project uses **uv** for Python project and dependency management.

### Required practices:
- Always use `uv run` to execute Python scripts (e.g., `uv run python script.py`)
- Use `uv add <package>` to add dependencies, never `pip install`
- Use `uv sync` to synchronize the environment from pyproject.toml
- Use `uv run pytest` for tests, `uv run mypy` for type checking, etc.
- Never activate virtual environments manually; `uv run` handles this
- If creating a new project, use `uv init`
