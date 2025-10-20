"""
Entry point for running streamduck as a module.

Usage:
    python -m streamduck [COMMAND] [OPTIONS]
"""

from .main import cli_main

if __name__ == "__main__":
    cli_main()
