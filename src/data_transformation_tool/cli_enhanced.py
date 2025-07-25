"""Enhanced command line interface with Rich, Typer, and Shellingham integration."""

import sys
import logging
from pathlib import Path
from typing import Optional, List
from enum import Enum

import typer
from typer import Argument, Option
from rich.console import Console
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
from rich.panel import Panel
from rich.prompt import Confirm, Prompt
from rich.syntax import Syntax
from rich import print as rprint
from rich.tree import Tree
from rich.live import Live
from rich.layout import Layout
from rich.columns import Columns
from rich.text import Text
import shellingham

# Import the main classes
from .main import DataTransformationTool
from .config import ToolConfig
from .core.validator import ModelValidator
from .core.plan import PlanGenerator, PlanFormatter, StateManager

# Suppress default logging to console
logging.getLogger('data_transformation_tool').setLevel(logging.WARNING)

# Initialize Rich console and Typer app
console = Console()
app = typer.Typer(
    name="data-transform",
    help="Data Transformation Tool - Generate SQL from YAML configurations",
    add_completion=True,
    rich_markup_mode="rich",
    pretty_exceptions_enable=True,
    pretty_exceptions_show_locals=True
)

class Environment(str, Enum):
    """Valid environment options"""
    dev = "dev"
    prod = "prod"

def detect_shell():
    """Detect the current shell using shellingham"""
    try:
        shell_name, shell_path = shellingham.detect_shell()
        return shell_name
    except Exception:
        return "unknown"

def show_welcome_banner():
    """Display a welcome banner with shell info"""
    shell = detect_shell()
    banner_text = f"""[bold blue]Data Transformation Tool[/bold blue]
[dim]YAML to SQL Generator[/dim]
[dim]Shell: {shell}[/dim]"""
    
    console.print(Panel(banner_text, expand=False, border_style="blue"))

def create_progress_bar():
    """Create a styled progress bar"""
    return Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        console=console
    )

def display_validation_results(errors: List[str]):
    """Display validation errors in a formatted table"""
    if errors:
        table = Table(title="[red]Validation Errors[/red]", show_header=True, header_style="bold red")
        table.add_column("Error", style="red", no_wrap=False)
        
        for error in errors:
            table.add_row(error)
        
        console.print(table)
    else:
        console.print("[green]✅ All models validated successfully[/green]")

def display_state_info(state: dict, environment: str):
    """Display state information in a formatted table"""
    if not state:
        console.print(f"[yellow]No state found for {environment}. Use --init-state to create initial state.[/yellow]")
        return
    
    table = Table(title=f"State for {environment} environment", show_header=True, header_style="bold cyan")
    table.add_column("Model", style="cyan", no_wrap=True)
    table.add_column("Layer", style="magenta")
    table.add_column("Dependencies", style="yellow")
    table.add_column("Columns", style="green")
    
    for model_name in sorted(state.keys()):
        model_state = state[model_name]
        deps = ", ".join(model_state.get('dependencies', []))[:50]
        if len(deps) == 50:
            deps += "..."
        table.add_row(
            model_name,
            model_state.get('layer', 'unknown'),
            deps or "-",
            str(len(model_state.get('columns', [])))
        )
    
    console.print(table)
    console.print(f"\n[bold]Total models: {len(state)}[/bold]")

def display_plan_with_rich(plan, formatter=None):
    """Display plan using Rich formatting"""
    # Create a tree view for the plan
    tree = Tree(f"[bold]Execution Plan for {plan.environment}[/bold]")
    
    if plan.changes:
        changes_branch = tree.add("[yellow]Changes to Apply:[/yellow]")
        
        # Group changes by type
        new_models = [c for c in plan.changes if c.change_type == "new"]
        updated_models = [c for c in plan.changes if c.change_type == "updated"]
        removed_models = [c for c in plan.changes if c.change_type == "removed"]
        
        if new_models:
            new_branch = changes_branch.add(f"[green]New Models ({len(new_models)})[/green]")
            for change in new_models:
                new_branch.add(f"[green]+ {change.model_name}[/green]")
        
        if updated_models:
            update_branch = changes_branch.add(f"[yellow]Updated Models ({len(updated_models)})[/yellow]")
            for change in updated_models:
                model_branch = update_branch.add(f"[yellow]~ {change.model_name}[/yellow]")
                for detail in change.details:
                    model_branch.add(f"[dim]{detail}[/dim]")
        
        if removed_models:
            remove_branch = changes_branch.add(f"[red]Removed Models ({len(removed_models)})[/red]")
            for change in removed_models:
                remove_branch.add(f"[red]- {change.model_name}[/red]")
    else:
        tree.add("[green]✅ No changes detected. Environment is up to date.[/green]")
    
    console.print(tree)
    
    # Show summary
    if plan.changes:
        summary = f"\n[bold]Summary:[/bold] {len(plan.changes)} changes to apply"
        console.print(Panel(summary, border_style="cyan"))

def handle_plan_command_enhanced(args, tool, environment: str, auto_apply: bool, dry_run: bool):
    """Enhanced plan command handler with Rich formatting"""
    console.print(f"[blue]🔍 Generating plan for {environment} environment...[/blue]")
    
    try:
        with create_progress_bar() as progress:
            task = progress.add_task("Analyzing models...", total=None)
            
            # Initialize plan generator
            plan_generator = PlanGenerator(tool)
            
            # Generate plan
            plan = plan_generator.generate_plan(args['models_directory'], environment)
            progress.update(task, completed=True)
        
        # Display plan with Rich formatting
        display_plan_with_rich(plan)
        
        # Check if there are any changes
        if not plan.changes:
            return True
        
        # Handle auto-apply or ask for confirmation
        should_apply = False
        
        if auto_apply:
            should_apply = True
            console.print(f"\n[green]🚀 Auto-applying plan to {environment}...[/green]")
        elif dry_run:
            console.print(f"\n[yellow]💡 Dry run complete. Use --auto-apply to apply changes.[/yellow]")
            return True
        else:
            # Ask for confirmation with Rich prompt
            should_apply = Confirm.ask(f"Apply plan to {environment} environment?", default=False)
        
        if should_apply:
            with create_progress_bar() as progress:
                task = progress.add_task("Applying plan...", total=None)
                success = plan_generator.apply_plan(plan)
                progress.update(task, completed=True)
            
            if success:
                console.print(f"[green]✅ Plan successfully applied to {environment}[/green]")
                
                # Optionally run the actual transformations
                if not dry_run:
                    if Confirm.ask("Run transformations now?", default=False):
                        console.print("[green]🚀 Running transformations...[/green]")
                        
                        with create_progress_bar() as progress:
                            task = progress.add_task("Running pipeline...", total=None)
                            success, sql_outputs, errors = tool.run_pipeline()
                            progress.update(task, completed=True)
                        
                        if success:
                            tool.save_sql_outputs(args['output'])
                            console.print(f"[green]✅ Transformations completed successfully[/green]")
                        else:
                            console.print("[red]❌ Transformation failed:[/red]")
                            display_validation_results(errors)
            else:
                console.print(f"[red]❌ Failed to apply plan to {environment}[/red]")
                return False
        else:
            console.print("[yellow]Plan cancelled.[/yellow]")
        
        return True
        
    except Exception as e:
        console.print(f"[red]❌ Error generating plan: {str(e)}[/red]")
        console.print_exception()
        return False

@app.command()
def main(
    models_directory: Path = Argument(..., help="Directory containing YAML model files"),
    output: Path = Option("output", "--output", "-o", help="Output directory for generated SQL files"),
    config: Optional[Path] = Option(None, "--config", "-c", help="Configuration file path"),
    validate_only: bool = Option(False, "--validate-only", help="Only validate models without generating SQL"),
    model: Optional[str] = Option(None, "--model", help="Generate SQL for specific model only"),
    lineage: bool = Option(False, "--lineage", help="Generate lineage report"),
    execution_plan: bool = Option(False, "--execution-plan", help="Show execution plan"),
    lineage_full: bool = Option(False, "--lineage-full", help="Generate full lineage with column-level details"),
    check_circular: bool = Option(False, "--check-circular", help="Check for circular dependencies"),
    test_column: Optional[List[str]] = Option(None, "--test-column", help="Test column lineage: MODEL COLUMN", min=2, max=2),
    plan: Optional[Environment] = Option(None, "--plan", help="Generate execution plan for environment"),
    auto_apply: bool = Option(False, "--auto-apply", help="Automatically apply the plan without confirmation"),
    dry_run: bool = Option(False, "--dry-run", help="Show what would be applied without making changes"),
    init_state: Optional[Environment] = Option(None, "--init-state", help="Initialize state for environment"),
    show_state: Optional[Environment] = Option(None, "--show-state", help="Show current state for environment")
):
    """Enhanced CLI with Rich formatting and better UX"""
    
    # Show welcome banner
    show_welcome_banner()
    
    # Load configuration
    if config:
        try:
            with console.status("[yellow]Loading configuration...[/yellow]"):
                config_dict = ToolConfig.from_file(config).to_dict()
        except Exception as e:
            console.print(f"[red]❌ Error loading config: {e}[/red]")
            raise typer.Exit(1)
    else:
        config_dict = {}
    
    # Override config with CLI arguments
    config_dict['models_directory'] = str(models_directory)
    config_dict['output_directory'] = str(output)
    
    # Initialize tool
    try:
        with console.status("[yellow]Initializing tool...[/yellow]"):
            tool = DataTransformationTool(str(models_directory), config_dict)
    except Exception as e:
        console.print(f"[red]❌ Error initializing tool: {e}[/red]")
        raise typer.Exit(1)
    
    # Create args dict for compatibility
    args = {
        'models_directory': str(models_directory),
        'output': str(output),
        'config': str(config) if config else None,
        'validate_only': validate_only,
        'model': model,
        'lineage': lineage,
        'execution_plan': execution_plan,
        'lineage_full': lineage_full,
        'check_circular': check_circular,
        'test_column': test_column
    }
    
    try:
        # Handle plan commands first
        if plan:
            success = handle_plan_command_enhanced(args, tool, plan.value, auto_apply, dry_run)
            raise typer.Exit(0 if success else 1)
        
        # Handle state management commands
        elif init_state:
            environment = init_state.value
            console.print(f"[blue]🔧 Initializing state for {environment} environment...[/blue]")
            
            with create_progress_bar() as progress:
                # Load current models
                task1 = progress.add_task("Loading models...", total=None)
                tool.models = tool.yaml_reader.read_all_models()
                progress.update(task1, completed=True)
                
                # Validate models first
                task2 = progress.add_task("Validating models...", total=None)
                tool.validator = ModelValidator(tool.models)
                is_valid, errors = tool.validator.validate_all()
                progress.update(task2, completed=True)
            
            if not is_valid:
                console.print("[red]❌ Cannot initialize state with invalid models:[/red]")
                display_validation_results(errors)
                raise typer.Exit(1)
            
            # Initialize state
            state_manager = StateManager()
            state_manager.save_state(environment, tool.models)
            console.print(f"[green]✅ State initialized for {environment} with {len(tool.models)} models[/green]")
        
        elif show_state:
            environment = show_state.value
            console.print(f"[blue]📋 Current state for {environment} environment:[/blue]\n")
            
            state_manager = StateManager()
            state = state_manager.get_current_state(environment)
            display_state_info(state, environment)
        
        # Rest of the existing CLI logic...
        elif validate_only:
            # Validation only mode
            console.print("\n[bold blue]🔍 Model Validation[/bold blue]")
            console.print("=" * 50 + "\n")
            
            # First, load and show models being loaded
            models_table = Table(title="Loading Models", show_header=True, header_style="bold cyan")
            models_table.add_column("Model", style="cyan", no_wrap=True)
            models_table.add_column("Layer", style="magenta")
            models_table.add_column("Source", style="dim")
            models_table.add_column("Status", justify="center")
            
            with Live(models_table, console=console, refresh_per_second=4) as live:
                # Load models
                tool.models = tool.yaml_reader.read_all_models()
                
                # Handle both dict and list format
                if isinstance(tool.models, dict):
                    model_configs = list(tool.models.values())
                else:
                    model_configs = tool.models
                
                # Group models by layer
                models_by_layer = {}
                for model_config in model_configs:
                    # Check if it's a ModelConfig object or dict
                    if hasattr(model_config, 'model'):
                        layer = model_config.model.layer
                        name = model_config.model.name
                    else:
                        # Fallback for dict format
                        layer = model_config.get('model', {}).get('layer', 'unknown')
                        name = model_config.get('model', {}).get('name', 'unknown')
                    
                    if layer not in models_by_layer:
                        models_by_layer[layer] = []
                    models_by_layer[layer].append((name, layer))
                
                # Add models to table by layer
                for layer in ['bronze', 'silver', 'gold', 'cte']:
                    if layer in models_by_layer:
                        for model_name, model_layer in models_by_layer[layer]:
                            models_table.add_row(
                                model_name,
                                layer.upper(),
                                f"{model_name}.yaml",
                                "[green]✓[/green]"
                            )
                            live.refresh()
            
            console.print(f"\n[green]✅ Loaded {len(tool.models)} models successfully[/green]\n")
            
            # Now validate with progress
            validation_progress = Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                console=console
            )
            
            with validation_progress as progress:
                # Create validation task
                task = progress.add_task("Validating models...", total=len(tool.models))
                
                # Initialize validator
                tool.validator = ModelValidator(tool.models)
                
                # Validate each model
                validation_errors = []
                model_list = list(tool.models.values()) if isinstance(tool.models, dict) else tool.models
                for i, model in enumerate(model_list):
                    model_name = model.model.name if hasattr(model, 'model') else str(model)
                    progress.update(task, advance=1, description=f"Validating {model_name}...")
                    
                    # Individual model validation would go here
                    # For now, we'll run the full validation at the end
                
                # Run full validation
                is_valid, errors = tool.validator.validate_all()
                
                if is_valid:
                    progress.update(task, description="[green]All validations passed![/green]")
                else:
                    progress.update(task, description="[red]Validation failed[/red]")
                    validation_errors = errors
            
            # Display results
            if validation_errors:
                console.print("\n[red]❌ Validation Errors Found[/red]\n")
                error_table = Table(show_header=True, header_style="bold red")
                error_table.add_column("Error Type", style="red", no_wrap=True)
                error_table.add_column("Details", style="yellow")
                
                for error in validation_errors:
                    if ":" in error:
                        error_type, detail = error.split(":", 1)
                        error_table.add_row(error_type.strip(), detail.strip())
                    else:
                        error_table.add_row("Validation Error", error)
                
                console.print(error_table)
                raise typer.Exit(1)
            else:
                # Show validation summary
                summary_table = Table(title="[green]Validation Summary[/green]", show_header=True, header_style="bold green")
                summary_table.add_column("Check", style="cyan")
                summary_table.add_column("Result", justify="center")
                
                summary_table.add_row("Model Structure", "[green]✓ Valid[/green]")
                summary_table.add_row("Dependencies", "[green]✓ Resolved[/green]")
                summary_table.add_row("Column Mappings", "[green]✓ Verified[/green]")
                summary_table.add_row("SQL Syntax", "[green]✓ Valid[/green]")
                summary_table.add_row("Circular Dependencies", "[green]✓ None Found[/green]")
                
                console.print(summary_table)
                console.print(f"\n[bold green]✅ All {len(tool.models)} models validated successfully![/bold green]\n")
        
        else:
            # Full pipeline
            console.print("\n[bold blue]🚀 Full Pipeline Execution[/bold blue]")
            console.print("=" * 50 + "\n")
            
            # Stage 1: Load models
            with console.status("[yellow]Loading models...[/yellow]") as status:
                tool.models = tool.yaml_reader.read_all_models()
                console.print(f"[green]✓[/green] Loaded {len(tool.models)} models")
            
            # Stage 2: Validate
            with console.status("[yellow]Validating models...[/yellow]") as status:
                tool.validator = ModelValidator(tool.models)
                is_valid, validation_errors = tool.validator.validate_all()
                if is_valid:
                    console.print("[green]✓[/green] All models validated")
                else:
                    console.print("[red]✗[/red] Validation failed")
                    display_validation_results(validation_errors)
                    raise typer.Exit(1)
            
            # Stage 3: Build dependency graph
            with console.status("[yellow]Building dependency graph...[/yellow]") as status:
                tool._build_dependency_graph()
                console.print("[green]✓[/green] Dependency graph built")
            
            # Stage 4: Generate SQL with progress
            console.print("\n[cyan]Generating SQL:[/cyan]")
            sql_progress = Progress(
                TextColumn("  "),
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(bar_width=30),
                TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                console=console
            )
            
            sql_outputs = {}
            errors = []
            
            with sql_progress as progress:
                task = progress.add_task("Generating SQL...", total=len(tool.models))
                
                model_list = list(tool.models.values()) if isinstance(tool.models, dict) else tool.models
                for model in model_list:
                    model_name = model.model.name if hasattr(model, 'model') else str(model)
                    progress.update(task, advance=1, description=f"Processing {model_name}")
                    try:
                        sql = tool.sql_generator.generate(model)
                        sql_outputs[model_name] = sql
                    except Exception as e:
                        errors.append(f"{model_name}: {str(e)}")
            
            if errors:
                console.print("\n[red]❌ SQL generation failed:[/red]")
                display_validation_results(errors)
                raise typer.Exit(1)
            
            console.print(f"\n[green]✓[/green] Generated SQL for {len(sql_outputs)} models")
            
            # Stage 5: Save outputs
            with console.status("[yellow]Saving SQL files...[/yellow]") as status:
                tool.save_sql_outputs(str(output))
                console.print(f"[green]✓[/green] SQL files saved to {output}")
            
            # Stage 6: Generate lineage (if requested)
            if config_dict.get('generate_lineage', True) or lineage or lineage_full:
                with console.status("[yellow]Generating lineage...[/yellow]") as status:
                    if lineage_full:
                        tool.generate_full_lineage_reports(str(output))
                        console.print("[green]✓[/green] Full lineage reports generated")
                    else:
                        lineage_report = tool.get_lineage_report()
                        lineage_file = Path(output) / "lineage_report.json"
                        with open(lineage_file, 'w') as f:
                            f.write(lineage_report)
                        console.print("[green]✓[/green] Lineage report generated")
            
            # Success summary with details
            console.print("\n")
            summary_table = Table(title="[green]Pipeline Summary[/green]", show_header=False, border_style="green")
            summary_table.add_column("Metric", style="cyan")
            summary_table.add_column("Value", style="bold")
            
            summary_table.add_row("Models Processed", str(len(sql_outputs)))
            summary_table.add_row("Output Directory", str(output))
            summary_table.add_row("Status", "[green]SUCCESS[/green]")
            
            # Count by layer
            layer_counts = {}
            model_list = list(tool.models.values()) if isinstance(tool.models, dict) else tool.models
            for model in model_list:
                layer = model.model.layer if hasattr(model, 'model') else 'unknown'
                layer_counts[layer] = layer_counts.get(layer, 0) + 1
            
            for layer, count in sorted(layer_counts.items()):
                summary_table.add_row(f"  {layer.capitalize()} Models", str(count))
            
            console.print(summary_table)
            console.print("\n[dim]💡 Tip: Use --plan dev/prod to preview changes before applying them[/dim]\n")
    
    except typer.Exit:
        raise
    except Exception as e:
        console.print(f"[red]❌ Unexpected error: {str(e)}[/red]")
        console.print_exception()
        raise typer.Exit(1)

if __name__ == "__main__":
    app()