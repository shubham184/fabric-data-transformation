"""Command line interface for the data transformation tool."""

import argparse
import sys
from pathlib import Path

# Import the main classes
from .main import DataTransformationTool
from .config import ToolConfig
from .core.validator import ModelValidator
from .core.plan import PlanGenerator, PlanFormatter, StateManager

def add_plan_arguments(parser):
    """Add plan-related arguments to the CLI parser"""
    
    # Plan command
    parser.add_argument(
        '--plan',
        metavar='ENVIRONMENT',
        help='Generate execution plan for environment (dev/prod)'
    )
    
    parser.add_argument(
        '--auto-apply',
        action='store_true',
        help='Automatically apply the plan without confirmation'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be applied without making changes'
    )

# Add this new function to handle plan commands:

def handle_plan_command(args, tool):
    """Handle plan-related commands"""
    if not args.plan:
        return False
    
    environment = args.plan
    print(f"🔍 Generating plan for {environment} environment...")
    
    try:
        # Initialize plan generator
        plan_generator = PlanGenerator(tool)
        
        # Generate plan
        plan = plan_generator.generate_plan(args.models_directory, environment)
        
        # Format and display plan
        formatted_plan = PlanFormatter.format_plan(plan)
        print(formatted_plan)
        
        # Check if there are any changes
        if not plan.changes:
            print("\n✅ No changes detected. Environment is up to date.")
            return True
        
        # Handle auto-apply or ask for confirmation
        should_apply = False
        
        if args.auto_apply:
            should_apply = True
            print(f"\n🚀 Auto-applying plan to {environment}...")
        elif args.dry_run:
            print(f"\n💡 Dry run complete. Use --auto-apply to apply changes.")
            return True
        else:
            # Ask for confirmation
            response = input(f"\nApply plan to {environment} environment? [y/N]: ").strip().lower()
            should_apply = response in ['y', 'yes']
        
        if should_apply:
            success = plan_generator.apply_plan(plan)
            if success:
                print(f"✅ Plan successfully applied to {environment}")
                
                # Optionally run the actual transformations
                if not args.dry_run:
                    run_response = input("Run transformations now? [y/N]: ").strip().lower()
                    if run_response in ['y', 'yes']:
                        print("🚀 Running transformations...")
                        success, sql_outputs, errors = tool.run_pipeline()
                        if success:
                            tool.save_sql_outputs(args.output)
                            print(f"✅ Transformations completed successfully")
                        else:
                            print("❌ Transformation failed:")
                            for error in errors:
                                print(f"  - {error}")
            else:
                print(f"❌ Failed to apply plan to {environment}")
                return False
        else:
            print("Plan cancelled.")
        
        return True
        
    except Exception as e:
        print(f"❌ Error generating plan: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Command line interface with plan support"""
    parser = argparse.ArgumentParser(
        description="Data Transformation Tool - Generate SQL from YAML configurations"
    )
    
    parser.add_argument(
        'models_directory',
        help='Directory containing YAML model files'
    )
    
    parser.add_argument(
        '--output', '-o',
        default='output',
        help='Output directory for generated SQL files'
    )
    
    parser.add_argument(
        '--config', '-c',
        help='Configuration file path'
    )
    
    parser.add_argument(
        '--validate-only',
        action='store_true',
        help='Only validate models without generating SQL'
    )
    
    parser.add_argument(
        '--model',
        help='Generate SQL for specific model only'
    )
    
    parser.add_argument(
        '--lineage',
        action='store_true',
        help='Generate lineage report'
    )
    
    parser.add_argument(
        '--execution-plan',
        action='store_true',
        help='Show execution plan'
    )
    
    # Enhanced lineage sub-options
    parser.add_argument(
        '--lineage-full',
        action='store_true',
        help='Generate full lineage with column-level details'
    )
    
    parser.add_argument(
        '--check-circular',
        action='store_true',
        help='Check for circular dependencies'
    )
    
    parser.add_argument(
        '--test-column',
        nargs=2,
        metavar=('MODEL', 'COLUMN'),
        help='Test column lineage: MODEL COLUMN'
    )
    
    # PLAN FEATURE ARGUMENTS
    parser.add_argument(
        '--plan',
        metavar='ENVIRONMENT',
        help='Generate execution plan for environment (dev/prod)'
    )
    
    parser.add_argument(
        '--auto-apply',
        action='store_true',
        help='Automatically apply the plan without confirmation'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be applied without making changes'
    )
    
    # State management commands
    parser.add_argument(
        '--init-state',
        metavar='ENVIRONMENT',
        help='Initialize state for environment with current models'
    )
    
    parser.add_argument(
        '--show-state',
        metavar='ENVIRONMENT',
        help='Show current state for environment'
    )
    
    args = parser.parse_args()
    
    # Load configuration
    if args.config:
        try:
            config = ToolConfig.from_file(args.config).to_dict()
        except Exception as e:
            print(f"❌ Error loading config: {e}")
            sys.exit(1)
    else:
        config = {}
    
    # Override config with CLI arguments
    config['models_directory'] = args.models_directory
    config['output_directory'] = args.output
    
    # Initialize tool
    try:
        tool = DataTransformationTool(args.models_directory, config)
    except Exception as e:
        print(f"❌ Error initializing tool: {e}")
        sys.exit(1)
    
    try:
        # Handle plan commands first
        if args.plan:
            success = handle_plan_command(args, tool)
            sys.exit(0 if success else 1)
        
        # Handle state management commands
        elif args.init_state:
            environment = args.init_state
            print(f"🔧 Initializing state for {environment} environment...")
            
            # Load current models
            tool.models = tool.yaml_reader.read_all_models()
            
            # Validate models first
            tool.validator = ModelValidator(tool.models)
            is_valid, errors = tool.validator.validate_all()
            
            if not is_valid:
                print("❌ Cannot initialize state with invalid models:")
                for error in errors:
                    print(f"  - {error}")
                sys.exit(1)
            
            # Initialize state
            state_manager = StateManager()
            state_manager.save_state(environment, tool.models)
            print(f"✅ State initialized for {environment} with {len(tool.models)} models")
        
        elif args.show_state:
            environment = args.show_state
            print(f"📋 Current state for {environment} environment:")
            
            state_manager = StateManager()
            state = state_manager.get_current_state(environment)
            
            if not state:
                print(f"No state found for {environment}. Use --init-state to create initial state.")
            else:
                print(f"Models: {len(state)}")
                for model_name in sorted(state.keys()):
                    model_state = state[model_name]
                    print(f"  - {model_name}")
                    print(f"    Layer: {model_state.get('layer', 'unknown')}")
                    print(f"    Dependencies: {model_state.get('dependencies', [])}")
                    print(f"    Columns: {len(model_state.get('columns', []))}")
        
        # Rest of the existing CLI logic...
        elif args.validate_only:
            # Validation only mode
            print("🔍 Validating models...")
            success, _, errors = tool.run_pipeline()
            if success:
                print("✅ All models validated successfully")
                sys.exit(0)
            else:
                print("❌ Validation failed:")
                for error in errors:
                    print(f"  - {error}")
                sys.exit(1)
        
        # ... [rest of existing CLI logic remains the same] ...
        
        else:
            # Full pipeline
            print("🚀 Running full pipeline...")
            success, sql_outputs, errors = tool.run_pipeline()
            
            if success:
                tool.save_sql_outputs(args.output)
                
                if config.get('generate_lineage', True):
                    lineage_report = tool.get_lineage_report()
                    lineage_file = Path(args.output) / "lineage_report.json"
                    with open(lineage_file, 'w') as f:
                        f.write(lineage_report)
                
                print(f"✅ Successfully generated SQL for {len(sql_outputs)} models")
                print(f"📁 Output saved to {args.output}")
                
                # Suggest using plan feature for future changes
                print("\n💡 Tip: Use --plan dev/prod to preview changes before applying them")
            else:
                print("❌ Pipeline failed:")
                for error in errors:
                    print(f"  - {error}")
                sys.exit(1)
    
    except Exception as e:
        print(f"❌ Unexpected error: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()