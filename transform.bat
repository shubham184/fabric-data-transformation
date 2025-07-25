@echo off
REM Wrapper script for fabric-data-transformation CLI
REM This script automatically activates the virtual environment and runs the CLI

set SCRIPT_DIR=%~dp0
cd /d "%SCRIPT_DIR%"

REM Check if virtual environment exists
if not exist ".venv" (
    echo Virtual environment not found. Running setup...
    call setup.bat
    if errorlevel 1 (
        echo Setup failed. Please check the error messages above.
        exit /b 1
    )
)

REM Activate virtual environment and run the CLI
call .venv\Scripts\activate.bat
data-transform %*