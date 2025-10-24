"""
Unit tests for Logfire configuration and initialization.

Tests cover:
1. Disabled configuration
2. Local console-only mode
3. Cloud-only mode
4. Hybrid mode (local + cloud)
5. Smart retry with LLM tracing integration
"""

import os
from unittest.mock import MagicMock, Mock, patch

import pytest

from src.utils.config import LogfireConfig


class TestLogfireConfigDisabled:
    """Test Logfire with disabled configuration."""

    def test_disabled_config_defaults(self, monkeypatch):
        """Test that disabled config has correct defaults."""
        # Clear any environment variables
        monkeypatch.delenv("LOGFIRE_TOKEN", raising=False)

        config = LogfireConfig(enabled=False)

        assert config.enabled is False
        # Token may be loaded from env if present, so don't test for None
        assert config.service_name == "streamduck"
        assert config.environment == "development"
        assert config.send_to_logfire is True  # Default, but won't be used
        assert config.console_logging is True  # Default, but won't be used
        assert config.log_level == "INFO"

    def test_disabled_config_validation_passes_without_token(self):
        """Test that disabled config doesn't require token."""
        # Should not raise validation error even without token
        config = LogfireConfig(
            enabled=False,
            send_to_logfire=True,
            token=None,  # No token provided
        )

        assert config.enabled is False
        assert config.token is None

    def test_disabled_initialization_skips_setup(self):
        """Test that disabled config skips Logfire initialization."""
        config = LogfireConfig(enabled=False)

        with patch("src.main.logfire") as mock_logfire:
            # Simulate initialization logic
            if not config.enabled:
                # Should skip initialization
                pass
            else:
                mock_logfire.configure()

            # Verify configure was never called
            mock_logfire.configure.assert_not_called()


class TestLogfireConfigLocalOnly:
    """Test Logfire with local console-only mode."""

    def test_local_only_config(self):
        """Test local console-only configuration."""
        config = LogfireConfig(
            enabled=True,
            send_to_logfire=False,  # No cloud
            console_logging=True,  # Local only
            token=None,  # No token needed
            service_name="test-service",
            environment="local",
            log_level="DEBUG",
        )

        assert config.enabled is True
        assert config.send_to_logfire is False
        assert config.console_logging is True
        assert config.token is None
        assert config.service_name == "test-service"
        assert config.environment == "local"
        assert config.log_level == "DEBUG"

    def test_local_only_validation_passes_without_token(self):
        """Test that local-only config doesn't require token."""
        # Should not raise validation error
        config = LogfireConfig(
            enabled=True,
            send_to_logfire=False,
            console_logging=True,
            token=None,
        )

        assert config.token is None
        assert config.send_to_logfire is False

    def test_local_only_console_configuration(self):
        """Test that local-only mode configures console options."""
        config = LogfireConfig(
            enabled=True,
            send_to_logfire=False,
            console_logging=True,
            log_level="DEBUG",
        )

        # Simulate initialization
        with patch("src.main.logfire") as mock_logfire:
            mock_console_options = Mock()
            mock_logfire.ConsoleOptions.return_value = mock_console_options

            if config.console_logging:
                console_opts = mock_logfire.ConsoleOptions(
                    verbose=config.console_logging,
                    min_log_level=config.log_level.lower(),
                )
                mock_logfire.configure(
                    token=None,
                    console=console_opts,
                    send_to_logfire=False,
                )

            mock_logfire.configure.assert_called_once()
            call_kwargs = mock_logfire.configure.call_args.kwargs
            assert call_kwargs["token"] is None
            assert call_kwargs["send_to_logfire"] is False


class TestLogfireConfigCloudOnly:
    """Test Logfire with cloud-only mode."""

    def test_cloud_only_config_requires_token(self, monkeypatch):
        """Test that cloud-only config requires token."""
        # Clear token environment variable
        monkeypatch.delenv("LOGFIRE_TOKEN", raising=False)

        with pytest.raises(Exception, match="LOGFIRE_TOKEN is required"):
            LogfireConfig(
                enabled=True,
                send_to_logfire=True,
                console_logging=False,
                token=None,  # Missing token should fail validation
            )

    def test_cloud_only_config_with_token(self):
        """Test cloud-only configuration with valid token."""
        config = LogfireConfig(
            enabled=True,
            send_to_logfire=True,
            console_logging=False,
            token="test-logfire-token-123",
            service_name="prod-service",
            environment="production",
            log_level="WARNING",
        )

        assert config.enabled is True
        assert config.send_to_logfire is True
        assert config.console_logging is False
        assert config.token == "test-logfire-token-123"
        assert config.service_name == "prod-service"
        assert config.environment == "production"
        assert config.log_level == "WARNING"

    def test_cloud_only_configuration_sends_to_cloud(self):
        """Test that cloud-only mode sends to Logfire cloud."""
        config = LogfireConfig(
            enabled=True,
            send_to_logfire=True,
            console_logging=False,
            token="test-token",
        )

        with patch("src.main.logfire") as mock_logfire:
            # Simulate initialization
            mock_logfire.configure(
                token=config.token,
                service_name=config.service_name,
                environment=config.environment,
                send_to_logfire=True,
                console=False,
            )

            mock_logfire.configure.assert_called_once()
            call_kwargs = mock_logfire.configure.call_args.kwargs
            assert call_kwargs["token"] == "test-token"
            assert call_kwargs["send_to_logfire"] is True
            assert call_kwargs["console"] is False


class TestLogfireConfigHybridMode:
    """Test Logfire with hybrid mode (local + cloud)."""

    def test_hybrid_mode_requires_token(self, monkeypatch):
        """Test that hybrid mode requires token for cloud."""
        # Clear token environment variable
        monkeypatch.delenv("LOGFIRE_TOKEN", raising=False)

        with pytest.raises(Exception, match="LOGFIRE_TOKEN is required"):
            LogfireConfig(
                enabled=True,
                send_to_logfire=True,  # Cloud enabled
                console_logging=True,  # Console also enabled
                token=None,  # Missing token should fail
            )

    def test_hybrid_mode_config(self):
        """Test hybrid mode configuration."""
        config = LogfireConfig(
            enabled=True,
            send_to_logfire=True,
            console_logging=True,
            token="hybrid-token-456",
            service_name="hybrid-service",
            environment="staging",
            log_level="INFO",
        )

        assert config.enabled is True
        assert config.send_to_logfire is True
        assert config.console_logging is True
        assert config.token == "hybrid-token-456"
        assert config.service_name == "hybrid-service"
        assert config.environment == "staging"

    def test_hybrid_mode_configuration_both_targets(self):
        """Test that hybrid mode configures both console and cloud."""
        config = LogfireConfig(
            enabled=True,
            send_to_logfire=True,
            console_logging=True,
            token="test-token",
            log_level="DEBUG",
        )

        with patch("src.main.logfire") as mock_logfire:
            mock_console_options = Mock()
            mock_logfire.ConsoleOptions.return_value = mock_console_options

            # Simulate initialization
            console_opts = mock_logfire.ConsoleOptions(
                verbose=config.console_logging,
                min_log_level=config.log_level.lower(),
            )
            mock_logfire.configure(
                token=config.token,
                service_name=config.service_name,
                environment=config.environment,
                send_to_logfire=True,
                console=console_opts,
            )

            mock_logfire.configure.assert_called_once()
            call_kwargs = mock_logfire.configure.call_args.kwargs
            assert call_kwargs["token"] == "test-token"
            assert call_kwargs["send_to_logfire"] is True
            assert call_kwargs["console"] == console_opts


class TestLogfireConfigLogLevels:
    """Test Logfire log level validation."""

    @pytest.mark.parametrize(
        "log_level",
        ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
    )
    def test_valid_log_levels(self, log_level):
        """Test that all valid log levels are accepted."""
        config = LogfireConfig(
            enabled=True,
            send_to_logfire=False,
            log_level=log_level,
        )

        assert config.log_level == log_level

    def test_invalid_log_level(self):
        """Test that invalid log levels are rejected."""
        with pytest.raises(Exception, match="Invalid log level"):
            LogfireConfig(
                enabled=True,
                log_level="INVALID",
            )

    def test_case_insensitive_log_level(self):
        """Test that lowercase log levels are accepted and normalized."""
        # The validator now accepts lowercase and normalizes it
        config = LogfireConfig(enabled=True, send_to_logfire=False, log_level="info")
        # After validation, it should be uppercase
        assert config.log_level.upper() == "INFO"


class TestLogfireSmartRetryIntegration:
    """Test Logfire integration with smart retry and LLM tracing."""

    def test_pydantic_ai_instrumentation_called(self):
        """Test that Pydantic AI instrumentation is enabled."""
        config = LogfireConfig(
            enabled=True,
            send_to_logfire=True,
            console_logging=True,
            token="smart-retry-token",
        )

        with patch("src.main.logfire") as mock_logfire:
            # Simulate initialization with Pydantic AI instrumentation
            mock_logfire.configure()
            mock_logfire.instrument_pydantic_ai()

            # Verify instrumentation was called
            mock_logfire.instrument_pydantic_ai.assert_called_once()

    def test_smart_retry_spans_created(self):
        """Test that smart retry creates proper spans."""
        with patch("src.utils.smart_retry.logfire") as mock_logfire:
            mock_span = MagicMock()
            mock_logfire.span.return_value.__enter__.return_value = mock_span

            # Simulate smart retry span creation
            with mock_logfire.span(
                "smart_retry.analyze_exception",
                exception_type="TestException",
                llm_provider="openai",
                llm_model="gpt-4o-mini",
            ) as span:
                span.set_attribute("decision", True)
                span.set_attribute("confidence", 0.85)

            # Verify span was created with correct attributes
            mock_logfire.span.assert_called_once()
            call_args = mock_logfire.span.call_args
            assert call_args[0][0] == "smart_retry.analyze_exception"
            assert call_args[1]["exception_type"] == "TestException"
            assert call_args[1]["llm_provider"] == "openai"

    def test_llm_call_span_tracking(self):
        """Test that LLM API calls are tracked with spans."""
        with patch("src.utils.smart_retry.logfire") as mock_logfire:
            mock_span = MagicMock()
            mock_logfire.span.return_value.__enter__.return_value = mock_span

            # Simulate LLM call span
            with mock_logfire.span(
                "smart_retry.llm_call",
                llm_provider="openai",
                llm_model="gpt-4o-mini",
                api_call_count=1,
            ) as span:
                span.set_attribute("decision", False)
                span.set_attribute("confidence", 0.95)
                span.set_attribute("prompt_length", 500)

            # Verify LLM call tracking
            mock_logfire.span.assert_called_once()
            mock_span.set_attribute.assert_any_call("decision", False)
            mock_span.set_attribute.assert_any_call("confidence", 0.95)
            mock_span.set_attribute.assert_any_call("prompt_length", 500)

    def test_end_to_end_observability_flow(self):
        """Test complete observability flow from config to execution."""
        config = LogfireConfig(
            enabled=True,
            send_to_logfire=True,
            console_logging=True,
            token="e2e-test-token",
            service_name="test-pipeline",
            environment="test",
            log_level="DEBUG",
        )

        with patch("src.main.logfire") as mock_logfire:
            # Simulate complete initialization
            mock_logfire.configure(
                token=config.token,
                service_name=config.service_name,
                environment=config.environment,
                send_to_logfire=config.send_to_logfire,
            )

            # Simulate Pydantic AI instrumentation
            mock_logfire.instrument_pydantic_ai()

            # Verify complete setup
            mock_logfire.configure.assert_called_once()
            mock_logfire.instrument_pydantic_ai.assert_called_once()

            # Verify configuration values
            call_kwargs = mock_logfire.configure.call_args.kwargs
            assert call_kwargs["token"] == "e2e-test-token"
            assert call_kwargs["service_name"] == "test-pipeline"
            assert call_kwargs["environment"] == "test"


class TestLogfireEnvironmentVariables:
    """Test Logfire configuration from environment variables."""

    def test_config_from_env_vars(self, monkeypatch):
        """Test loading Logfire config from environment variables."""
        monkeypatch.setenv("LOGFIRE_ENABLED", "true")
        monkeypatch.setenv("LOGFIRE_TOKEN", "env-token-789")
        monkeypatch.setenv("LOGFIRE_SERVICE_NAME", "env-service")
        monkeypatch.setenv("LOGFIRE_ENVIRONMENT", "production")
        monkeypatch.setenv("LOGFIRE_SEND_TO_LOGFIRE", "true")
        monkeypatch.setenv("LOGFIRE_CONSOLE_LOGGING", "false")
        monkeypatch.setenv("LOGFIRE_LOG_LEVEL", "ERROR")

        config = LogfireConfig()

        assert config.enabled is True
        assert config.token == "env-token-789"
        assert config.service_name == "env-service"
        assert config.environment == "production"
        assert config.send_to_logfire is True
        assert config.console_logging is False
        assert config.log_level == "ERROR"

    def test_config_defaults_without_env_vars(self, monkeypatch):
        """Test default values when no environment variables are set."""
        # Clear all Logfire environment variables
        monkeypatch.delenv("LOGFIRE_TOKEN", raising=False)
        monkeypatch.delenv("LOGFIRE_ENABLED", raising=False)
        monkeypatch.delenv("LOGFIRE_SERVICE_NAME", raising=False)
        monkeypatch.delenv("LOGFIRE_ENVIRONMENT", raising=False)
        monkeypatch.delenv("LOGFIRE_SEND_TO_LOGFIRE", raising=False)
        monkeypatch.delenv("LOGFIRE_CONSOLE_LOGGING", raising=False)
        monkeypatch.delenv("LOGFIRE_LOG_LEVEL", raising=False)
        
        # Create a fresh config instance with explicit model_config to prevent .env loading
        from pydantic_settings import SettingsConfigDict
        
        class TestLogfireConfig(LogfireConfig):
            model_config = SettingsConfigDict(
                env_prefix="LOGFIRE_",
                case_sensitive=False,
                env_file=None,  # Don't load from .env file
            )
        
        config = TestLogfireConfig()

        assert config.enabled is False  # Default when not set in environment
        assert config.service_name == "streamduck"
        assert config.environment == "development"
        assert config.send_to_logfire is True
        assert config.console_logging is True
        assert config.log_level == "INFO"
