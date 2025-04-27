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

# Data Pipeline Usage

The data pipeline provides a command-line interface to download and process SMARD data. All commands use `uv` as the package manager.

## Setup

1. Install dependencies:
```bash
uv sync
```

## Download Data

The pipeline supports downloading different categories of SMARD data:

```bash
# Download specific categories
uv run python datapipe.py download generation  # Power generation data
uv run python datapipe.py download consumption # Power consumption data
uv run python datapipe.py download prices      # Market price data
uv run python datapipe.py download forecasts   # Forecast data

# Download all data
uv run python datapipe.py download all

# Force redownload (ignore existing files)
uv run python datapipe.py download generation --force
```

## Run Full Pipeline

To run all pipeline steps in sequence:

```bash
uv run python datapipe.py pipeline
```

Add `--force` flag to rerun all steps regardless of existing outputs:

```bash
uv run python datapipe.py pipeline --force
```

## Generate Reports

You can convert Python analysis scripts into HTML reports using the `generate_report.py` script. This is useful for sharing data analysis results in a readable format.

```bash
# Generate HTML report from a Python script
uv run python tests/generate_report.py data_analysis.py

# This will create:
# - output/reports/analyze_generation.ipynb (intermediate notebook)
# - output/reports/analyze_generation.html (final report)
```

The script will:
1. Convert your Python script to a Jupyter notebook
2. Execute the notebook to capture outputs
3. Generate an HTML report with results

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