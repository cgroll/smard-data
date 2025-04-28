# Python rules

```
You are an AI assistant specialized in Python development. Generate Python code and guidance adhering to these rules:

- Keep It Simple: Write simple and clear code; avoid unnecessary complexity.
- Use Type Hints: Utilize type hints for better code clarity and type checking for every function.
- Documentation: Use Google Style docstrings and maintain a README.md.
- Error Handling: Implement robust try/except blocks. Log errors with context.
- Structure: Use standard directories (src/, tests/, docs/).
- Modularity: Separate concerns into distinct files (models, services, utils, etc.).
- Configuration: Use environment variables ONLY. No hardcoding.
- Testing: Write comprehensive tests using pytest.
- Code Style: Enforce style using Ruff.
- Code Clarity: Use descriptive names, type hints, minimal necessary comments, and provide rich error context.
- Comments: Add inline comments (#) before or within sections with complex or non-obvious logic to improve readability.
```

> Note: For the latest version of these rules, please refer to `.cursor/rules/python-development.md`

# Setup

1. Install dependencies:
```bash
uv sync
```

# Data Pipeline Usage

The data pipeline provides a command-line interface to download and process SMARD data.

## Download Data & Generate Reports

The pipeline uses DVC to manage data downloads and report generation. The workflow is defined in `dvc.yaml`.

```bash
uv run dvc repro
```

Not all outputs of the stages are "cached". This keeps the dvc cache to a minimum. The pipline also create several report outputs that visualize the results from the data analysis script and are a good starting point to create a full markdown blog post.

# Development

## Run ruff checks

```bash
uvx ruff check
```

## Run unit tests

```bash
uv run pytest tests -v
uv run pytest tests/test_api.py -v
```