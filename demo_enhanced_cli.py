#!/usr/bin/env python3
"""Demo script showing the enhanced CLI features"""

import subprocess
import sys

def main():
    """Run demo commands to showcase enhanced CLI"""
    
    print("🎨 Data Transformation Tool - Enhanced CLI Demo")
    print("=" * 50)
    print("\nThis demo showcases the new Rich-enhanced CLI features:")
    print("- Beautiful formatted output with colors")
    print("- Progress bars for long operations")
    print("- Interactive prompts with better UX")
    print("- Formatted tables for state display")
    print("- Tree view for execution plans")
    print("\nTo use the enhanced CLI after installation:")
    print("  data-transform-rich [command] [options]")
    print("\nOr directly via Python:")
    print("  python -m data_transformation_tool.cli_enhanced [command] [options]")
    print("\nExamples:")
    print("  # Show help with rich formatting")
    print("  data-transform-rich --help")
    print("\n  # Validate models with progress bar")
    print("  data-transform-rich path/to/models --validate-only")
    print("\n  # Show state with formatted table")
    print("  data-transform-rich path/to/models --show-state dev")
    print("\n  # Generate plan with tree view")
    print("  data-transform-rich path/to/models --plan dev")
    print("\n  # Run pipeline with progress tracking")
    print("  data-transform-rich path/to/models --output ./output")

if __name__ == "__main__":
    main()