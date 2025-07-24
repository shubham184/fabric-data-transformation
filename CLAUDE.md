# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a YAML-to-SQL data transformation tool designed for medallion architecture data pipelines. It converts declarative YAML model definitions into optimized SQL for various database platforms.

## Build and Development Commands

### Installation
```bash
# Install in development mode with all dependencies
pip install -e ".[dev]"
```

### Code Quality Commands
```bash
# Format code
black src/

# Type checking
mypy src/data_transformation_tool/

# Linting
flake8 src/

# Run all quality checks
black src/ && flake8 src/ && mypy src/data_transformation_tool/
```

### Testing
```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=data_transformation_tool --cov-report=html

# Run specific test markers
pytest -m unit        # Unit tests only
pytest -m integration # Integration tests only
pytest -m "not slow"  # Skip slow tests

# Run a single test file
pytest tests/test_specific.py::test_function_name
```

## Architecture Overview

### Core Components

1. **Model System** (`src/data_transformation_tool/core/models.py`):
   - Pydantic models define the structure of YAML configurations
   - Key models: `ModelConfig`, `SourceConfig`, `TransformationConfig`, `QualityConfig`
   - Strict validation ensures YAML correctness before SQL generation

2. **Dependency Resolution** (`src/data_transformation_tool/core/dependency_graph.py`):
   - Uses NetworkX to build and resolve model dependencies
   - Handles circular dependency detection
   - Generates execution order for models

3. **SQL Generation** (`src/data_transformation_tool/sql/generator.py`):
   - Converts validated models to SQL using Jinja2 templates
   - Supports multiple dialects (Postgres, Spark) via dialect system
   - Handles CTEs, joins, aggregations, and incremental strategies

4. **Execution Planning** (`src/data_transformation_tool/core/plan.py`):
   - Tracks model state across environments (dev/prod)
   - Generates deployment plans showing what will change
   - Supports dry-run and auto-apply modes

### Layer Structure

The tool implements medallion architecture with three layers:
- **Bronze**: Raw data ingestion (`test_models_v2/bronze/`)
- **Silver**: Cleaned and standardized data (`test_models_v2/silver/`)
- **Gold**: Business-ready aggregations (`test_models_v2/gold/`)
- **CTE**: Reusable common table expressions (`test_models_v2/cte/`)

### Key Design Patterns

1. **Dialect Pattern**: SQL generation varies by database type
   - Base dialect class defines interface
   - Specific dialects (Postgres, Spark) override methods
   - Optimizers apply dialect-specific improvements

2. **Strategy Pattern**: Incremental processing strategies
   - `append`: Add new records only
   - `merge`: Upsert based on keys
   - `delete_insert`: Replace partitions

3. **Visitor Pattern**: Lineage tracking traverses model graph
   - Builds column-level dependencies
   - Exports to multiple formats (DOT, JSON, HTML)

## CLI Usage Patterns

### Basic Operations
```bash
# Generate SQL for all models
data-transform path/to/models --output ./sql_output

# Validate models without generating SQL
data-transform path/to/models --validate-only

# Use specific SQL dialect
data-transform path/to/models --output ./output --dialect spark

# Generate lineage visualization
data-transform path/to/models --output ./output --export-lineage
```

### Environment Management
```bash
# Initialize state for an environment
data-transform path/to/models --init-state dev

# Show current state
data-transform path/to/models --show-state dev

# Generate deployment plan
data-transform path/to/models --plan dev

# Apply plan with auto-confirmation
data-transform path/to/models --plan prod --auto-apply

# Dry run to see changes
data-transform path/to/models --plan dev --dry-run
```

## YAML Model Structure

Models are defined in YAML with these sections:
- `model`: Metadata (name, layer, owner, tags)
- `source`: Data sources and dependencies
- `transformations`: Column mappings and expressions
- `filters`: WHERE clause conditions
- `aggregations`: GROUP BY logic
- `quality`: Data quality rules
- `relationships`: Foreign key definitions
- `incremental`: Processing strategy

See `docs/yaml_config.md` for comprehensive YAML documentation.

## Extension Points

1. **New SQL Dialects**: Add to `src/data_transformation_tool/sql/dialects/`
2. **Quality Rules**: Add to `src/data_transformation_tool/quality/rules/`
3. **Export Formats**: Add to `src/data_transformation_tool/lineage/exporters/`
4. **Incremental Strategies**: Add to `src/data_transformation_tool/incremental/strategies/`

## Common Development Tasks

### Adding a New Feature
1. Define/update Pydantic models in `core/models.py`
2. Update validator in `core/validator.py` if needed
3. Implement logic in appropriate module
4. Update SQL templates if SQL generation changes
5. Add tests (when test framework is set up)

### Debugging Model Issues
1. Use `--validate-only` to check YAML syntax
2. Check dependency graph with lineage export
3. Use `--plan` to preview changes before applying
4. Enable verbose logging in `utils/logging_utils.py`

### Working with State
State files track deployed models per environment:
- Location: `.fabric_state/{environment}.json`
- Contains model signatures and deployment timestamps
- Used for incremental deployments and rollbacks