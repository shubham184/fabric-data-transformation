# Quick Start Guide

## One-Time Setup

### On Unix/Linux/macOS:
```bash
./setup.sh
```

### On Windows:
```cmd
setup.bat
```

This will automatically:
- Create a Python virtual environment
- Install all dependencies
- Set up the `data-transform` CLI

## Using the CLI

### Option 1: Direct Usage (Recommended)

#### Unix/Linux/macOS:
```bash
./transform.sh path/to/models --output ./sql_output
```

#### Windows:
```cmd
transform.bat path\to\models --output .\sql_output
```

The wrapper scripts automatically handle virtual environment activation.

### Option 2: Manual Activation

#### Unix/Linux/macOS:
```bash
source .venv/bin/activate
data-transform path/to/models --output ./sql_output
```

#### Windows:
```cmd
.venv\Scripts\activate.bat
data-transform path\to\models --output .\sql_output
```

## Examples

Generate SQL for all models:
```bash
./transform.sh test_models_v2 --output ./output
```

Validate models only:
```bash
./transform.sh test_models_v2 --validate-only
```

Generate with lineage visualization:
```bash
./transform.sh test_models_v2 --output ./output --export-lineage
```

## Troubleshooting

If you encounter issues:
1. Ensure Python 3.8+ is installed: `python3 --version`
2. Delete `.venv` folder and run setup again
3. Check error messages for missing dependencies