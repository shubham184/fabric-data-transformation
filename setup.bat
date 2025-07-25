@echo off
REM Automated setup script for fabric-data-transformation CLI

echo Setting up Fabric Data Transformation Tool...

REM Check if Python is installed
python --version >nul 2>&1
if errorlevel 1 (
    echo Python 3 is not installed. Please install Python 3.8 or higher.
    exit /b 1
)

REM Get Python version
for /f "tokens=2" %%i in ('python --version 2^>^&1') do set PYTHON_VERSION=%%i
echo Found Python %PYTHON_VERSION%

REM Create virtual environment if it doesn't exist
if not exist ".venv" (
    echo Creating virtual environment...
    python -m venv .venv
    echo Virtual environment created
) else (
    echo Virtual environment already exists
)

REM Activate virtual environment
echo Activating virtual environment...
call .venv\Scripts\activate.bat

REM Upgrade pip
echo Upgrading pip...
python -m pip install --upgrade pip setuptools wheel

REM Install the package in development mode
echo Installing fabric-data-transformation...
pip install -e ".[dev]"

echo.
echo Setup complete!
echo.
echo The 'data-transform' CLI is now available in this virtual environment.
echo.
echo To use the CLI, run:
echo   .venv\Scripts\activate.bat
echo   data-transform --help
echo.
echo Or use the wrapper script:
echo   transform.bat --help