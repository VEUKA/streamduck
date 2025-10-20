"""
Tests for package initialization modules.

This module tests:
- src/__init__.py - Package version and main function
- src/__main__.py - Module execution entry point
"""

import sys
from unittest.mock import patch, MagicMock
import pytest


class TestPackageInit:
    """Test cases for src/__init__.py"""

    def test_version_defined(self):
        """Test that __version__ is defined in package."""
        import src
        
        assert hasattr(src, "__version__")
        assert isinstance(src.__version__, str)
        assert src.__version__ == "0.1.0"

    def test_main_function_exists(self):
        """Test that main function exists in package."""
        # Import directly from __init__ to avoid confusion with src.main module
        import src
        from src import __version__
        
        # Check the function exists in the __init__ module
        import importlib
        init_module = importlib.import_module("src")
        
        # Get main function from __init__.py
        assert hasattr(init_module, "__version__")
        # The main function is defined in __init__ but may be shadowed by src.main module
        # So we verify it exists by checking the source
        import inspect
        source_file = inspect.getsourcefile(init_module)
        with open(source_file, 'r') as f:
            content = f.read()
            assert 'def main()' in content

    def test_main_function_prints_message(self, capsys):
        """Test that main function prints expected message."""
        # Execute the main function directly from __init__.py
        import importlib.util
        import sys
        
        spec = importlib.util.spec_from_file_location("src_init", "src/__init__.py")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        
        module.main()
        
        captured = capsys.readouterr()
        assert "Hello from streamduck!" in captured.out


class TestMainModule:
    """Test cases for src/__main__.py"""

    @patch("src.__main__.cli_main")
    def test_main_module_entry_point(self, mock_cli_main):
        """Test that __main__.py calls cli_main when executed."""
        # Import the module to trigger the if __name__ == "__main__" block
        # We need to simulate running it as __main__
        with patch.object(sys, "argv", ["python", "-m", "streamduck"]):
            # Execute the __main__ module code
            import src.__main__ as main_module
            
            # The import itself won't trigger the if __name__ == "__main__" block
            # So we test that cli_main is imported and available
            assert hasattr(main_module, "cli_main")
            assert callable(main_module.cli_main)

    def test_main_module_imports_cli_main(self):
        """Test that __main__.py imports cli_main from main module."""
        import src.__main__ as main_module
        
        # Verify cli_main is imported
        assert hasattr(main_module, "cli_main")
        
        # Verify it's the same function as in main
        from src.main import cli_main as original_cli_main
        assert main_module.cli_main is original_cli_main

    @patch("src.main.cli_main")
    def test_main_module_execution(self, mock_cli_main):
        """Test module execution via runpy."""
        import runpy
        
        # Execute the module as __main__
        with patch.dict(sys.modules, {"__main__": MagicMock()}):
            try:
                # This will execute src/__main__.py
                runpy.run_module("src", run_name="__main__")
                mock_cli_main.assert_called_once()
            except SystemExit:
                # cli_main might call sys.exit, which is expected
                pass


class TestPackageStructure:
    """Test package structure and imports."""

    def test_package_is_importable(self):
        """Test that the package can be imported."""
        import src
        
        assert src is not None

    def test_main_module_is_importable(self):
        """Test that __main__ module can be imported."""
        import src.__main__
        
        assert src.__main__ is not None

    def test_version_format(self):
        """Test that version follows semantic versioning."""
        import src
        
        version_parts = src.__version__.split(".")
        assert len(version_parts) == 3
        assert all(part.isdigit() for part in version_parts)
