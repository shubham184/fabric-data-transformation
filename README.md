# Data Transformation Tool

A YAML-to-SQL data transformation tool for medallion architecture.

## Installation

```bash
pip install -e .
```

## Quick Start

```bash
# Generate SQL for all models
data-transform path/to/models --output ./sql_output

# Validate models only
data-transform path/to/models --validate-only
```

## Documentation

See `docs/` directory for detailed documentation.
