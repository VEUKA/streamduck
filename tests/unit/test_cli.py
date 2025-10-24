"""
Tests for CLI commands in src/main.py.

This module provides comprehensive test coverage for all CLI commands
and helper functions in the main CLI application.
"""

import json
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest
from typer.testing import CliRunner

from src.main import app
from src.utils.config import StreamDuckConfig


# Initialize CLI test runner
runner = CliRunner()


# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture
def mock_config(sample_eventhub_config, sample_motherduck_config, sample_mapping):
    """Create a mock StreamDuckConfig for testing."""
    config = MagicMock(spec=StreamDuckConfig)
    config.event_hubs = {"EVENTHUBNAME_1": sample_eventhub_config}
    config.motherduck_configs = {"MOTHERDUCK_1": sample_motherduck_config}
    config.mappings = [sample_mapping]
    config.get_event_hub_config.return_value = sample_eventhub_config
    config.get_motherduck_config.return_value = sample_motherduck_config
    config.validate_configuration.return_value = {
        "valid": True,
        "errors": [],
        "warnings": [],
        "event_hubs_count": 1,
        "motherduck_configs_count": 1,
        "mappings_count": 1,
    }
    # Add logfire config mock
    config.logfire = MagicMock()
    config.logfire.enabled = False
    config.logfire.send_to_logfire = False
    config.logfire.console_logging = True
    config.logfire.log_level = "INFO"
    config.logfire.service_name = "streamduck"
    config.logfire.environment = "test"
    return config


@pytest.fixture
def mock_invalid_config():
    """Create a mock invalid config for testing error cases."""
    config = MagicMock(spec=StreamDuckConfig)
    config.mappings = []  # Invalid config has no mappings
    config.validate_configuration.return_value = {
        "valid": False,
        "errors": ["Invalid configuration: Missing required field"],
        "warnings": ["Warning: Using default values"],
        "event_hubs_count": 0,
        "motherduck_configs_count": 0,
        "mappings_count": 0,
    }
    return config


# ============================================================================
# Test: Version Command
# ============================================================================


class TestVersionCommand:
    """Test suite for the version command."""

    def test_version_command(self):
        """Test that version command displays correct version information."""
        result = runner.invoke(app, ["version"])
        
        assert result.exit_code == 0
        assert "StreamDuck v0.1.0" in result.output
        assert "EventHub to MotherDuck streaming pipeline" in result.output
        assert "Azure EventHub async consumer" in result.output
        assert "MotherDuck high-performance ingestion" in result.output

    def test_version_command_components(self):
        """Test that version command shows all components."""
        result = runner.invoke(app, ["version"])
        
        assert result.exit_code == 0
        assert "Components:" in result.output
        assert "Pipeline orchestrator" in result.output


# ============================================================================
# Test: Validate Config Command
# ============================================================================


class TestValidateConfigCommand:
    """Test suite for the validate-config command."""

    @patch("src.main.typer.confirm")
    @patch("src.main.load_config")
    def test_validate_config_success(self, mock_load_config, mock_confirm, mock_config):
        """Test validate-config command with valid configuration."""
        mock_load_config.return_value = mock_config
        mock_confirm.return_value = False  # Don't show detailed config
        
        result = runner.invoke(app, ["validate-config"])
        
        assert result.exit_code == 0
        assert "Configuration is valid" in result.output
        mock_load_config.assert_called_once()

    @patch("src.main.load_config")
    def test_validate_config_failure(self, mock_load_config):
        """Test validate-config command with invalid configuration."""
        mock_load_config.side_effect = ValueError("Invalid config: missing fields")
        
        result = runner.invoke(app, ["validate-config"])
        
        assert result.exit_code == 1
        assert "Configuration error" in result.output

    @patch("src.main.typer.confirm")
    @patch("src.main.load_config")
    def test_validate_config_with_errors(self, mock_load_config, mock_confirm, mock_invalid_config):
        """Test validate-config command displays errors."""
        mock_load_config.return_value = mock_invalid_config
        mock_confirm.return_value = False
        
        result = runner.invoke(app, ["validate-config"])
        
        assert result.exit_code == 0
        assert "Configuration has errors" in result.output
        assert "Invalid configuration" in result.output

    @patch("src.main.typer.confirm")
    @patch("src.main.load_config")
    def test_validate_config_with_warnings(self, mock_load_config, mock_confirm, mock_config):
        """Test validate-config command displays warnings."""
        mock_config.validate_configuration.return_value = {
            "valid": True,
            "errors": [],
            "warnings": ["Warning: Using default batch size"],
            "event_hubs_count": 1,
            "motherduck_configs_count": 1,
            "mappings_count": 1,
        }
        mock_load_config.return_value = mock_config
        mock_confirm.return_value = False
        
        result = runner.invoke(app, ["validate-config"])
        
        assert result.exit_code == 0
        assert "Warnings:" in result.output
        assert "Using default batch size" in result.output

    @patch("src.main.typer.confirm")
    @patch("src.main.load_config")
    def test_validate_config_with_env_file(self, mock_load_config, mock_confirm, mock_config):
        """Test validate-config command with custom env file."""
        mock_load_config.return_value = mock_config
        mock_confirm.return_value = False
        
        result = runner.invoke(app, ["validate-config", "--env-file", ".env.test"])
        
        assert result.exit_code == 0
        mock_load_config.assert_called_once_with(".env.test")

    @patch("src.main.typer.confirm")
    @patch("src.main.load_config")
    @patch("src.main._show_rbac_guidance")
    def test_validate_config_with_show_rbac(self, mock_show_rbac, mock_load_config, mock_confirm, mock_config):
        """Test validate-config command with --show-rbac flag."""
        mock_load_config.return_value = mock_config
        mock_confirm.return_value = False
        
        result = runner.invoke(app, ["validate-config", "--show-rbac"])
        
        assert result.exit_code == 0
        mock_show_rbac.assert_called_once()

    @patch("src.main.typer.confirm")
    @patch("src.main.load_config")
    def test_validate_config_control_table_verification(self, mock_load_config, mock_confirm, mock_config, monkeypatch):
        """Test validate-config verifies control table."""
        mock_load_config.return_value = mock_config
        mock_confirm.return_value = False
        
        # Set environment variables for control table
        # Note: Since we can't easily mock the duckdb connection, this test
        # will show a warning which is acceptable for this test case
        monkeypatch.setenv("TARGET_DB", "test_db")
        monkeypatch.setenv("TARGET_SCHEMA", "test_schema")
        monkeypatch.setenv("TARGET_TABLE", "test_table")
        monkeypatch.setenv("MOTHERDUCK_TOKEN", "test_token")
        
        result = runner.invoke(app, ["validate-config"])
        
        assert result.exit_code == 0
        # Since create_control_table tries to actually connect, it will fail
        # and show a warning, which is expected behavior
        assert "Verifying control table" in result.output

    @patch("src.main.typer.confirm")
    @patch("src.main.load_config")
    @patch("src.utils.motherduck.create_control_table")
    def test_validate_config_control_table_warning(self, mock_create_table, mock_load_config, mock_confirm, mock_config, monkeypatch):
        """Test validate-config handles control table creation failure."""
        mock_load_config.return_value = mock_config
        mock_create_table.side_effect = Exception("Connection error")
        mock_confirm.return_value = False
        
        # Set environment variables for control table
        monkeypatch.setenv("TARGET_DB", "test_db")
        monkeypatch.setenv("TARGET_SCHEMA", "test_schema")
        monkeypatch.setenv("TARGET_TABLE", "test_table")
        monkeypatch.setenv("MOTHERDUCK_TOKEN", "test_token")
        
        result = runner.invoke(app, ["validate-config"])
        
        assert result.exit_code == 0
        assert "Could not verify control table" in result.output

    @patch("src.main.typer.confirm")
    @patch("src.main.load_config")
    def test_validate_config_summary_table(self, mock_load_config, mock_confirm, mock_config):
        """Test validate-config displays configuration summary table."""
        mock_load_config.return_value = mock_config
        mock_confirm.return_value = False
        
        result = runner.invoke(app, ["validate-config"])
        
        assert result.exit_code == 0
        assert "Configuration Summary" in result.output
        assert "Event Hubs" in result.output
        assert "MotherDuck Configs" in result.output
        assert "Mappings" in result.output


# ============================================================================
# Test: Run Command
# ============================================================================


class TestRunCommand:
    """Test suite for the run command."""

    @patch("src.pipeline.orchestrator.run_pipeline")
    @patch("src.main.load_config")
    def test_run_command_success(self, mock_load_config, mock_run_pipeline, mock_config):
        """Test run command with valid configuration."""
        mock_load_config.return_value = mock_config
        mock_run_pipeline.return_value = None
        
        result = runner.invoke(app, ["run"])
        
        assert result.exit_code == 0
        assert "Starting StreamDuck Pipeline" in result.output
        mock_load_config.assert_called_once()

    @patch("src.main.load_config")
    def test_run_command_with_config_errors(self, mock_load_config, mock_invalid_config):
        """Test run command with invalid configuration."""
        mock_load_config.return_value = mock_invalid_config
        
        result = runner.invoke(app, ["run"])
        
        assert result.exit_code == 1
        assert "Configuration has errors" in result.output

    @patch("src.main.load_config")
    def test_run_command_with_warnings(self, mock_load_config, mock_config):
        """Test run command displays warnings."""
        mock_config.validate_configuration.return_value = {
            "valid": True,
            "errors": [],
            "warnings": ["Warning: High batch size may impact memory"],
            "event_hubs_count": 1,
            "motherduck_configs_count": 1,
            "mappings_count": 1,
        }
        mock_load_config.return_value = mock_config
        
        # Use dry-run to avoid actually running the pipeline
        result = runner.invoke(app, ["run", "--dry-run"])
        
        assert result.exit_code == 0
        assert "Configuration warnings:" in result.output

    @patch("src.main.load_config")
    @patch("src.main._show_processing_plan")
    def test_run_command_dry_run(self, mock_show_plan, mock_load_config, mock_config):
        """Test run command in dry-run mode."""
        mock_load_config.return_value = mock_config
        
        result = runner.invoke(app, ["run", "--dry-run"])
        
        assert result.exit_code == 0
        assert "DRY RUN MODE" in result.output
        mock_show_plan.assert_called_once_with(mock_config)

    @patch("src.main.load_config")
    def test_run_command_with_env_file(self, mock_load_config, mock_config):
        """Test run command with custom env file."""
        mock_load_config.return_value = mock_config
        
        result = runner.invoke(app, ["run", "--env-file", ".env.test", "--dry-run"])
        
        assert result.exit_code == 0
        mock_load_config.assert_called_once_with(".env.test")

    @patch("src.pipeline.orchestrator.run_pipeline")
    @patch("src.main.load_config")
    def test_run_command_keyboard_interrupt(self, mock_load_config, mock_run_pipeline, mock_config):
        """Test run command handles keyboard interrupt gracefully."""
        mock_load_config.return_value = mock_config
        
        # Mock asyncio.run to raise KeyboardInterrupt
        import asyncio
        original_run = asyncio.run
        
        def mock_asyncio_run(coro):
            raise KeyboardInterrupt()
        
        with patch("asyncio.run", side_effect=KeyboardInterrupt()):
            result = runner.invoke(app, ["run"])
        
        assert result.exit_code == 0
        assert "Pipeline stopped by user" in result.output

    @patch("src.pipeline.orchestrator.run_pipeline")
    @patch("src.main.load_config")
    def test_run_command_authentication_error(self, mock_load_config, mock_run_pipeline, mock_config):
        """Test run command handles authentication errors."""
        mock_load_config.return_value = mock_config
        
        # Mock asyncio.run to raise authentication error
        with patch("asyncio.run", side_effect=Exception("AuthenticationError: not authorized")):
            result = runner.invoke(app, ["run"])
        
        assert result.exit_code == 1
        assert "Authentication/Permission Error" in result.output
        assert "Azure RBAC permissions" in result.output

    @patch("src.pipeline.orchestrator.run_pipeline")
    @patch("src.main.load_config")
    def test_run_command_generic_error(self, mock_load_config, mock_run_pipeline, mock_config):
        """Test run command handles generic errors."""
        mock_load_config.return_value = mock_config
        
        # Mock asyncio.run to raise generic error
        with patch("asyncio.run", side_effect=Exception("Connection timeout")):
            result = runner.invoke(app, ["run"])
        
        assert result.exit_code == 1
        assert "Pipeline error" in result.output
        assert "TROUBLESHOOTING" in result.output

    @patch("src.main.load_config")
    def test_run_command_load_config_error(self, mock_load_config):
        """Test run command handles config loading errors."""
        mock_load_config.side_effect = FileNotFoundError("Config file not found")
        
        result = runner.invoke(app, ["run"])
        
        assert result.exit_code == 1


# ============================================================================
# Test: Status Command
# ============================================================================


class TestStatusCommand:
    """Test suite for the status command."""

    @patch("src.utils.motherduck.check_connection")
    @patch("src.main.load_config")
    def test_status_command_success(self, mock_load_config, mock_check_conn, mock_config):
        """Test status command with valid configuration."""
        mock_load_config.return_value = mock_config
        mock_check_conn.return_value = True
        
        result = runner.invoke(app, ["status"])
        
        assert result.exit_code == 0
        assert "Pipeline Status Check" in result.output
        assert "Configuration is valid" in result.output

    @patch("src.main.load_config")
    def test_status_command_invalid_config(self, mock_load_config, mock_invalid_config):
        """Test status command with invalid configuration."""
        mock_load_config.return_value = mock_invalid_config
        
        result = runner.invoke(app, ["status"])
        
        assert result.exit_code == 0
        assert "Configuration has errors" in result.output

    @patch("utils.motherduck.check_connection")
    @patch("src.main.load_config")
    def test_status_command_motherduck_connection_success(self, mock_load_config, mock_check_conn, mock_config, monkeypatch):
        """Test status command with successful MotherDuck connection."""
        mock_load_config.return_value = mock_config
        mock_check_conn.return_value = True
        
        # Set required environment variables to avoid validation errors
        monkeypatch.setenv("MOTHERDUCK_TOKEN", "test_token")
        monkeypatch.setenv("TARGET_DB", "test_db")
        monkeypatch.setenv("TARGET_SCHEMA", "test_schema")
        monkeypatch.setenv("TARGET_TABLE", "test_table")
        
        result = runner.invoke(app, ["status"])
        
        assert result.exit_code == 0
        assert "MotherDuck connection successful" in result.output

    @patch("utils.motherduck.check_connection")
    @patch("src.main.load_config")
    def test_status_command_motherduck_connection_failure(self, mock_load_config, mock_check_conn, mock_config, monkeypatch):
        """Test status command with failed MotherDuck connection."""
        mock_load_config.return_value = mock_config
        mock_check_conn.return_value = False
        
        # Set required environment variables to avoid validation errors
        monkeypatch.setenv("MOTHERDUCK_TOKEN", "test_token")
        monkeypatch.setenv("TARGET_DB", "test_db")
        monkeypatch.setenv("TARGET_SCHEMA", "test_schema")
        monkeypatch.setenv("TARGET_TABLE", "test_table")
        
        result = runner.invoke(app, ["status"])
        
        assert result.exit_code == 0
        assert "MotherDuck connection failed" in result.output

    @patch("src.main.load_config")
    def test_status_command_motherduck_connection_error(self, mock_load_config, mock_config):
        """Test status command handles MotherDuck connection errors."""
        mock_load_config.return_value = mock_config
        
        # Patch check_connection where it's imported (inside the status function)
        with patch("utils.motherduck.check_connection", side_effect=Exception("Connection timeout")):
            result = runner.invoke(app, ["status"])
        
        assert result.exit_code == 0
        assert "MotherDuck connection error" in result.output

    @patch("src.main.load_config")
    def test_status_command_with_env_file(self, mock_load_config, mock_config):
        """Test status command with custom env file."""
        mock_load_config.return_value = mock_config
        
        result = runner.invoke(app, ["status", "--env-file", ".env.test"])
        
        assert result.exit_code == 0
        mock_load_config.assert_called_once_with(".env.test")

    @patch("src.main.load_config")
    def test_status_command_shows_configured_mappings(self, mock_load_config, mock_config):
        """Test status command displays configured mappings table."""
        mock_load_config.return_value = mock_config
        
        result = runner.invoke(app, ["status"])
        
        assert result.exit_code == 0
        assert "Configured Mappings" in result.output

    @patch("src.main.load_config")
    def test_status_command_error_handling(self, mock_load_config):
        """Test status command handles errors gracefully."""
        mock_load_config.side_effect = Exception("Unexpected error")
        
        result = runner.invoke(app, ["status"])
        
        assert result.exit_code == 1
        assert "Status check error" in result.output


# ============================================================================
# Test: Check Credentials Command
# ============================================================================


class TestCheckCredentialsCommand:
    """Test suite for the check-credentials command."""

    @patch("azure.identity.AzureCliCredential")
    @patch("azure.identity.ManagedIdentityCredential")
    @patch("azure.identity.EnvironmentCredential")
    @patch("subprocess.run")
    def test_check_credentials_all_available(self, mock_subprocess, mock_env_cred, mock_msi_cred, mock_cli_cred):
        """Test check-credentials with all credentials available."""
        # Mock all credentials as available
        mock_env_cred.return_value = MagicMock()
        mock_msi_cred.return_value = MagicMock()
        mock_cli_cred.return_value = MagicMock()
        mock_subprocess.return_value = MagicMock(returncode=0, stdout="user@example.com\n")
        
        result = runner.invoke(app, ["check-credentials"])
        
        assert result.exit_code == 0
        assert "Checking Available Azure Credentials" in result.output

    @patch("azure.identity.AzureCliCredential")
    @patch("azure.identity.ManagedIdentityCredential")
    @patch("azure.identity.EnvironmentCredential")
    def test_check_credentials_env_available(self, mock_env_cred, mock_msi_cred, mock_cli_cred):
        """Test check-credentials with environment credentials available."""
        mock_env_cred.return_value = MagicMock()
        mock_msi_cred.side_effect = Exception("Not in Azure")
        mock_cli_cred.side_effect = Exception("CLI not logged in")
        
        result = runner.invoke(app, ["check-credentials"])
        
        assert result.exit_code == 0
        assert "Environment variables" in result.output

    @patch("azure.identity.AzureCliCredential")
    @patch("azure.identity.ManagedIdentityCredential")
    @patch("azure.identity.EnvironmentCredential")
    @patch("subprocess.run")
    def test_check_credentials_managed_identity(self, mock_subprocess, mock_env_cred, mock_msi_cred, mock_cli_cred):
        """Test check-credentials detects managed identity."""
        mock_env_cred.side_effect = Exception("No env vars")
        mock_msi_cred.return_value = MagicMock()
        mock_cli_cred.return_value = MagicMock()
        mock_subprocess.return_value = MagicMock(returncode=0, stdout="user@example.com\n")
        
        result = runner.invoke(app, ["check-credentials"])
        
        assert result.exit_code == 0
        assert "Managed Identity" in result.output
        assert "IMPORTANT" in result.output

    @patch("azure.identity.AzureCliCredential")
    @patch("azure.identity.ManagedIdentityCredential")
    @patch("azure.identity.EnvironmentCredential")
    def test_check_credentials_cli_only(self, mock_env_cred, mock_msi_cred, mock_cli_cred):
        """Test check-credentials with only CLI credentials."""
        mock_env_cred.side_effect = Exception("No env vars")
        mock_msi_cred.side_effect = Exception("Not in Azure")
        mock_cli_cred.return_value = MagicMock()
        
        result = runner.invoke(app, ["check-credentials"])
        
        assert result.exit_code == 0
        assert "Azure CLI" in result.output
        assert "Available" in result.output
        assert "Will be used for authentication" in result.output


# ============================================================================
# Test: Monitor Command
# ============================================================================


class TestMonitorCommand:
    """Test suite for the monitor command."""

    def test_monitor_command_not_implemented(self):
        """Test monitor command shows not implemented message."""
        result = runner.invoke(app, ["monitor"])
        
        assert result.exit_code == 1
        assert "Monitor UI not yet implemented" in result.output

    def test_monitor_command_with_log_file(self):
        """Test monitor command with log file option."""
        result = runner.invoke(app, ["monitor", "--log-file", "test.log"])
        
        assert result.exit_code == 1


# ============================================================================
# Test: Helper Functions
# ============================================================================


class TestHelperFunctions:
    """Test suite for CLI helper functions."""

    @patch("src.main.console")
    def test_show_rbac_guidance(self, mock_console):
        """Test _show_rbac_guidance displays RBAC information."""
        from src.main import _show_rbac_guidance
        
        _show_rbac_guidance()
        
        # Verify console.print was called with RBAC information
        assert mock_console.print.called
        call_args = [str(call[0]) for call in mock_console.print.call_args_list]
        output_text = " ".join(call_args)
        assert "Azure RBAC" in output_text or "RBAC" in output_text

    @patch("src.main.console")
    def test_show_detailed_config(self, mock_console, mock_config):
        """Test _show_detailed_config displays configuration details."""
        from src.main import _show_detailed_config
        
        _show_detailed_config(mock_config)
        
        # Verify console.print was called
        assert mock_console.print.called

    @patch("src.main.console")
    def test_show_processing_plan(self, mock_console, mock_config):
        """Test _show_processing_plan displays processing plan."""
        from src.main import _show_processing_plan
        
        _show_processing_plan(mock_config)
        
        # Verify console.print was called
        assert mock_console.print.called
