"""
Main CLI application for StreamDuck pipeline.

This module provides the main entry point for the pipeline that continuously:
1. Reads from Event Hub topics (configured via environment variables)
2. Sends messages to MotherDuck tables (one table per topic)

Based on TYPER CLI framework with configuration management via pydantic settings.
"""

import logging
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.logging import RichHandler
from rich.table import Table

# Load .env file by default if it exists
try:
    from dotenv import load_dotenv

    # Look for .env file in current directory or parent directories
    env_path = Path.cwd() / ".env"
    if not env_path.exists():
        # Look for .env in the project root (where pyproject.toml is)
        project_root = Path(__file__).parent.parent.parent
        env_path = project_root / ".env"

    if env_path.exists():
        load_dotenv(env_path)
except ImportError:
    pass  # python-dotenv not installed, environment variables should be set manually

from utils.config import StreamDuckConfig, load_config

# Initialize CLI app and console
app = typer.Typer(
    name="streamduck",
    help="StreamDuck - Event Hub to MotherDuck streaming pipeline",
    add_completion=False,
)
console = Console()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    datefmt="[%X]",
    handlers=[RichHandler(console=console, rich_tracebacks=True)],
)
logger = logging.getLogger(__name__)


@app.command()
def validate_config(
    env_file: Optional[str] = typer.Option(
        None,
        "--env-file",
        "-e",
        help="Path to environment file (.env)",
    ),
) -> None:
    """Validate the configuration and display summary."""
    try:
        console.print("[bold blue]Loading configuration...[/bold blue]")

        config = load_config(env_file)
        validation_results = config.validate_configuration()

        if validation_results["valid"]:
            console.print("[bold green]✓ Configuration is valid![/bold green]")

            # Check and create control table if needed
            try:
                import os

                from utils.motherduck import create_control_table

                target_db = os.getenv("TARGET_DB")
                target_schema = os.getenv("TARGET_SCHEMA")
                target_table = os.getenv("TARGET_TABLE")

                if target_db and target_schema and target_table:
                    console.print(
                        f"\n[bold blue]Verifying control table:[/bold blue] {target_db}.{target_schema}.{target_table}"
                    )

                    if create_control_table(
                        target_db=target_db,
                        target_schema=target_schema,
                        target_table=target_table,
                        env_file=env_file,
                    ):
                        console.print(
                            "[green]✓ Control table verified/created successfully[/green]"
                        )
                    else:
                        console.print(
                            "[yellow]⚠ Warning: Could not verify control table[/yellow]"
                        )
                else:
                    console.print(
                        "[yellow]⚠ Control table settings not found in environment (TARGET_DB, TARGET_SCHEMA, TARGET_TABLE)[/yellow]"
                    )

            except Exception as e:
                console.print(
                    f"[yellow]⚠ Warning: Could not verify control table: {e}[/yellow]"
                )
                logger.warning(f"Control table verification failed: {e}")
        else:
            console.print("[bold red]✗ Configuration has errors![/bold red]")

        # Create summary table
        table = Table(title="Configuration Summary")
        table.add_column("Component", style="cyan")
        table.add_column("Count", justify="right", style="magenta")

        table.add_row("Event Hubs", str(validation_results["event_hubs_count"]))
        table.add_row(
            "MotherDuck Configs", str(validation_results["motherduck_configs_count"])
        )
        table.add_row("Mappings", str(validation_results["mappings_count"]))

        console.print(table)

        # Display warnings
        if validation_results["warnings"]:
            console.print("\n[bold yellow]Warnings:[/bold yellow]")
            for warning in validation_results["warnings"]:
                console.print(f"  [yellow]⚠[/yellow] {warning}")

        # Display errors
        if validation_results["errors"]:
            console.print("\n[bold red]Errors:[/bold red]")
            for error in validation_results["errors"]:
                console.print(f"  [red]✗[/red] {error}")

        # Display detailed configurations
        if typer.confirm("\nShow detailed configuration?", default=False):
            _show_detailed_config(config)

    except Exception as e:
        console.print(f"[bold red]Configuration error:[/bold red] {e}")
        raise typer.Exit(1)


@app.command()
def run(
    env_file: Optional[str] = typer.Option(
        None,
        "--env-file",
        "-e",
        help="Path to environment file (.env)",
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Validate configuration and show what would be processed without actually running",
    ),
) -> None:
    """Run the ELT pipeline continuously."""
    try:
        console.print("[bold blue]Starting StreamDuck Pipeline...[/bold blue]")

        # Load and validate configuration
        config = load_config(env_file)
        validation_results = config.validate_configuration()

        if not validation_results["valid"] and validation_results["errors"]:
            console.print(
                "[bold red]Configuration has errors. Please fix them first.[/bold red]"
            )
            for error in validation_results["errors"]:
                console.print(f"  [red]✗[/red] {error}")
            raise typer.Exit(1)

        if validation_results["warnings"]:
            console.print("[yellow]Configuration warnings:[/yellow]")
            for warning in validation_results["warnings"]:
                console.print(f"  [yellow]⚠[/yellow] {warning}")

        if dry_run:
            console.print(
                "\n[bold yellow]DRY RUN MODE - No actual processing will occur[/bold yellow]"
            )
            _show_processing_plan(config)
            return

        # Start the pipeline
        console.print(
            f"\n[green]Starting pipeline with {len(config.mappings)} mappings...[/green]"
        )

        # Import here to avoid circular imports
        import asyncio

        from pipeline.orchestrator import run_pipeline

        # Run the async pipeline
        asyncio.run(run_pipeline(config))

    except KeyboardInterrupt:
        console.print("\n[yellow]Pipeline stopped by user[/yellow]")
        raise typer.Exit(0)
    except Exception as e:
        console.print(f"[bold red]Pipeline error:[/bold red] {e}")
        logger.exception("Unexpected error in pipeline")
        raise typer.Exit(1)


def _show_detailed_config(config: StreamDuckConfig) -> None:
    """Display detailed configuration information."""

    # Event Hubs
    if config.event_hubs:
        console.print("\n[bold cyan]Event Hub Configurations:[/bold cyan]")
        for key, eh_config in config.event_hubs.items():
            table = Table(title=f"Event Hub: {key}")
            table.add_column("Property", style="cyan")
            table.add_column("Value", style="white")

            table.add_row("Name", eh_config.name)
            table.add_row("Namespace", eh_config.namespace)
            table.add_row("Consumer Group", eh_config.consumer_group)
            table.add_row("Max Batch Size", str(eh_config.max_batch_size))
            table.add_row("Max Wait Time", f"{eh_config.max_wait_time}s")
            table.add_row("Prefetch Count", str(eh_config.prefetch_count))

            console.print(table)

    # MotherDuck Configs
    if config.motherduck_configs:
        console.print("\n[bold cyan]MotherDuck Configurations:[/bold cyan]")
        for key, md_config in config.motherduck_configs.items():
            table = Table(title=f"MotherDuck: {key}")
            table.add_column("Property", style="cyan")
            table.add_column("Value", style="white")

            table.add_row("Database", md_config.database)
            table.add_row("Schema", md_config.schema_name)
            table.add_row("Table", md_config.table_name)
            table.add_row("Batch Size", str(md_config.batch_size))
            table.add_row(
                "Token",
                "***" + md_config.token[-4:] if len(md_config.token) > 4 else "***",
            )

            console.print(table)

    # Mappings
    if config.mappings:
        console.print("\n[bold cyan]Event Hub ↔ MotherDuck Mappings:[/bold cyan]")
        for i, mapping in enumerate(config.mappings, 1):
            table = Table(title=f"Mapping {i}")
            table.add_column("Property", style="cyan")
            table.add_column("Value", style="white")

            table.add_row("Event Hub", mapping.event_hub_key)
            table.add_row("MotherDuck", mapping.motherduck_key)
            table.add_row("Channel Pattern", mapping.channel_name_pattern)

            console.print(table)


def _show_processing_plan(config: StreamDuckConfig) -> None:
    """Show what would be processed in dry-run mode."""
    console.print("\n[bold cyan]Processing Plan:[/bold cyan]")

    for mapping in config.mappings:
        eh_config = config.get_event_hub_config(mapping.event_hub_key)
        md_config = config.get_motherduck_config(mapping.motherduck_key)

        if eh_config and md_config:
            console.print(
                f"\n[green]Mapping:[/green] {mapping.event_hub_key} → {mapping.motherduck_key}"
            )
            console.print(
                f"  [cyan]Source:[/cyan] Event Hub '{eh_config.name}' in '{eh_config.namespace}'"
            )
            console.print(
                f"  [cyan]Target:[/cyan] MotherDuck '{md_config.database}.{md_config.schema_name}.{md_config.table_name}'"
            )
            console.print(
                f"  [cyan]Batch Size:[/cyan] {eh_config.max_batch_size} messages"
            )
            console.print(f"  [cyan]Max Wait:[/cyan] {eh_config.max_wait_time} seconds")


@app.command()
def status(
    env_file: Optional[str] = typer.Option(
        None,
        "--env-file",
        "-e",
        help="Path to environment file (.env)",
    ),
) -> None:
    """Show pipeline status and health check."""
    try:
        console.print("[bold blue]Pipeline Status Check[/bold blue]")

        # Load configuration
        config = load_config(env_file)
        validation_results = config.validate_configuration()

        # Show configuration status
        if validation_results["valid"]:
            console.print("[green]✓ Configuration is valid[/green]")
        else:
            console.print("[red]✗ Configuration has errors[/red]")

        # Show mapping summary
        table = Table(title="Configured Mappings")
        table.add_column("EventHub", style="cyan")
        table.add_column("MotherDuck", style="magenta")
        table.add_column("Status", style="green")

        for mapping in config.mappings:
            eh_config = config.get_event_hub_config(mapping.event_hub_key)
            md_config = config.get_motherduck_config(mapping.motherduck_key)

            if eh_config and md_config:
                table.add_row(
                    f"{eh_config.namespace}/{eh_config.name}",
                    f"{md_config.database}.{md_config.schema_name}.{md_config.table_name}",
                    "Ready" if validation_results["valid"] else "Config Error",
                )

        console.print(table)

        # Test connections if configuration is valid
        if validation_results["valid"]:
            console.print("\n[bold cyan]Testing Connections...[/bold cyan]")

            # Test MotherDuck connection
            try:
                from utils.motherduck import check_connection

                if check_connection():
                    console.print("[green]✓ MotherDuck connection successful[/green]")
                else:
                    console.print("[red]✗ MotherDuck connection failed[/red]")
            except Exception as e:
                console.print(f"[red]✗ MotherDuck connection error: {e}[/red]")

            console.print(
                "\n[yellow]Note: EventHub connections are tested during runtime[/yellow]"
            )

    except Exception as e:
        console.print(f"[bold red]Status check error:[/bold red] {e}")
        raise typer.Exit(1)


@app.command()
def monitor(
    log_file: Optional[str] = typer.Option(
        None,
        "--log-file",
        "-l",
        help="Path to log file (default: pipeline_monitor.log in current directory)",
    ),
) -> None:
    """Launch interactive monitoring UI for the pipeline."""
    try:
        console.print("[red]Monitor UI not yet implemented[/red]")
        raise typer.Exit(1)

    except Exception as e:
        console.print(f"[bold red]Monitor UI error:[/bold red] {e}")
        logger.exception("Error starting monitor UI")
        raise typer.Exit(1)


@app.command()
def version() -> None:
    """Show version information."""
    console.print("StreamDuck v0.1.0")
    console.print("EventHub to MotherDuck streaming pipeline")
    console.print("\nComponents:")
    console.print("  • Azure EventHub async consumer with custom checkpointing")
    console.print("  • MotherDuck high-performance ingestion")
    console.print("  • Pipeline orchestrator with concurrent mapping management")


@app.callback()
def main() -> None:
    """StreamDuck - EventHub to MotherDuck pipeline."""
    pass


def cli_main():
    """Entry point for the CLI when installed as a package."""
    app()


if __name__ == "__main__":
    app()
