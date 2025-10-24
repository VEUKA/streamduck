"""
Unit tests for smart retry functionality.

Tests the LLM-powered exception analyzer and retry decorators.
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic_ai.exceptions import ModelRetry, UnexpectedModelBehavior

from src.utils.smart_retry import (
    ExceptionAnalyzer,
    RetryDecision,
    RetryManager,
    create_smart_retry_decorator,
    create_standard_retry_decorator,
)


class TestRetryDecision:
    """Test RetryDecision Pydantic model."""

    def test_retry_decision_valid(self):
        """Test creating a valid retry decision."""
        decision = RetryDecision(
            should_retry=True,
            reasoning="Network timeout is transient",
            confidence=0.9,
            suggested_wait_seconds=30,
        )
        assert decision.should_retry is True
        assert decision.confidence == 0.9
        assert decision.suggested_wait_seconds == 30

    def test_retry_decision_defaults(self):
        """Test retry decision with default values."""
        decision = RetryDecision(
            should_retry=False,
            reasoning="Auth error is permanent",
        )
        assert decision.should_retry is False
        assert decision.confidence == 0.5  # default
        assert decision.suggested_wait_seconds == 2  # default (min is 1)


class TestExceptionAnalyzer:
    """Test ExceptionAnalyzer LLM integration."""

    @pytest.fixture
    def mock_llm_client(self):
        """Create a mock LLM client."""
        mock = MagicMock()
        mock.name.return_value = "mock-llm"
        return mock

    @pytest.fixture
    def analyzer(self, mock_llm_client, monkeypatch):
        """Create ExceptionAnalyzer with mock LLM."""
        # Set fake API key to avoid OpenAI initialization error
        monkeypatch.setenv("OPENAI_API_KEY", "test-api-key")
        
        analyzer = ExceptionAnalyzer(
            llm_provider="openai",
            llm_model="gpt-4o-mini",
            llm_api_key="test-api-key",
            timeout_seconds=10,
            enable_caching=True,
        )
        return analyzer

    @pytest.mark.asyncio
    async def test_analyze_exception_network_error(self, analyzer):
        """Test analyzing a network timeout error (should retry)."""
        exception = TimeoutError("Connection timed out after 30 seconds")
        context = {
            "operation": "ingest_batch",
            "attempt": 1,
            "max_attempts": 3,
        }

        # Mock the agent.run method
        with patch.object(analyzer.agent, "run", new_callable=AsyncMock) as mock_run:
            mock_result = AsyncMock()
            mock_result.output = RetryDecision(
                should_retry=True,
                reasoning="Network timeout is transient and should be retried",
                confidence=0.95,
                suggested_wait_seconds=30,
            )
            mock_run.return_value = mock_result

            decision = await analyzer.analyze_exception(exception, context)

            assert decision.should_retry is True
            assert decision.confidence >= 0.9
            assert "timeout" in decision.reasoning.lower() or "transient" in decision.reasoning.lower()
            assert decision.suggested_wait_seconds > 0

            # Verify agent was called with correct parameters
            mock_run.assert_called_once()

    @pytest.mark.asyncio
    async def test_analyze_exception_auth_error(self, analyzer):
        """Test analyzing an authentication error (should NOT retry)."""
        exception = PermissionError("Invalid credentials: Access denied")
        context = {
            "operation": "ingest_batch",
            "attempt": 1,
            "max_attempts": 3,
        }

        with patch.object(analyzer.agent, "run", new_callable=AsyncMock) as mock_run:
            mock_result = AsyncMock()
            mock_result.output = RetryDecision(
                should_retry=False,
                reasoning="Authentication errors are permanent and won't be fixed by retrying",
                confidence=0.98,
                suggested_wait_seconds=1,  # Min value per Field validation
            )
            mock_run.return_value = mock_result

            decision = await analyzer.analyze_exception(exception, context)

            assert decision.should_retry is False
            assert decision.confidence >= 0.9
            assert "auth" in decision.reasoning.lower() or "credential" in decision.reasoning.lower() or "permanent" in decision.reasoning.lower()
            assert decision.suggested_wait_seconds >= 0  # Can be 0 or 1

    @pytest.mark.asyncio
    async def test_analyze_exception_with_timeout(self, analyzer):
        """Test that analysis respects timeout."""
        exception = ValueError("Some error")
        context = {"operation": "test"}

        # Mock agent to raise timeout
        with patch.object(analyzer.agent, "run", new_callable=AsyncMock) as mock_run:
            mock_run.side_effect = asyncio.TimeoutError("LLM request timed out")

            decision = await analyzer.analyze_exception(exception, context)

            # Should return conservative decision (no retry)
            assert decision.should_retry is False
            assert "timeout" in decision.reasoning.lower() or "default" in decision.reasoning.lower()

    @pytest.mark.asyncio
    async def test_analyze_exception_llm_failure(self, analyzer):
        """Test fallback when LLM fails."""
        exception = ValueError("Some error")
        context = {"operation": "test"}

        # Mock agent to raise unexpected error
        with patch.object(analyzer.agent, "run", new_callable=AsyncMock) as mock_run:
            mock_run.side_effect = Exception("LLM service unavailable")

            decision = await analyzer.analyze_exception(exception, context)

            # Should return conservative decision (no retry)
            assert decision.should_retry is False
            assert "failed" in decision.reasoning.lower() or "default" in decision.reasoning.lower()

    @pytest.mark.asyncio
    async def test_analyze_exception_with_caching(self, analyzer):
        """Test that caching prevents duplicate LLM calls."""
        exception = TimeoutError("Connection timeout")
        context = {"operation": "test", "attempt": 1}

        with patch.object(analyzer.agent, "run", new_callable=AsyncMock) as mock_run:
            mock_result = AsyncMock()
            mock_result.output = RetryDecision(
                should_retry=True,
                reasoning="Network timeout",
                confidence=0.9,
                suggested_wait_seconds=30,
            )
            mock_run.return_value = mock_result

            # First call
            decision1 = await analyzer.analyze_exception(exception, context)
            assert decision1.should_retry is True

            # Second call with same exception (should use cache)
            decision2 = await analyzer.analyze_exception(exception, context)
            assert decision2.should_retry is True

            # Should only call LLM once due to caching
            assert mock_run.call_count == 1


class TestRetryDecorators:
    """Test retry decorators (standard and smart)."""

    @pytest.mark.asyncio
    async def test_standard_retry_success(self):
        """Test standard retry with successful operation."""
        call_count = 0

        @create_standard_retry_decorator(max_attempts=3)
        async def flaky_operation():
            nonlocal call_count
            call_count += 1
            return "success"

        result = await flaky_operation()
        assert result == "success"
        assert call_count == 1

    @pytest.mark.asyncio
    async def test_standard_retry_with_retries(self):
        """Test standard retry with transient failures."""
        call_count = 0

        @create_standard_retry_decorator(max_attempts=3)
        async def flaky_operation():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise TimeoutError("Network timeout")
            return "success"

        result = await flaky_operation()
        assert result == "success"
        assert call_count == 3

    @pytest.mark.asyncio
    async def test_standard_retry_exhausted(self):
        """Test standard retry when all attempts fail."""
        call_count = 0

        @create_standard_retry_decorator(max_attempts=3)
        async def always_fail():
            nonlocal call_count
            call_count += 1
            raise TimeoutError("Network timeout")

        with pytest.raises(TimeoutError):
            await always_fail()
        
        assert call_count == 3

    @pytest.mark.asyncio
    async def test_smart_retry_llm_says_retry(self):
        """Test smart retry when LLM recommends retry."""
        call_count = 0
        mock_analyzer = AsyncMock()
        mock_analyzer.analyze_exception.return_value = RetryDecision(
            should_retry=True,
            reasoning="Transient network error",
            confidence=0.9,
            suggested_wait_seconds=1,
        )

        # Use the correct signature: analyzer and max_attempts
        @create_smart_retry_decorator(
            analyzer=mock_analyzer,
            max_attempts=3,
        )
        async def flaky_operation():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise TimeoutError("Network timeout")
            return "success"

        result = await flaky_operation()
        assert result == "success"
        assert call_count == 3
        # Note: Tenacity may call the predicate multiple times per retry attempt
        # Verify it was called at least once for each failure (2 failures minimum)
        assert mock_analyzer.analyze_exception.call_count >= 2, \
            f"Expected at least 2 calls, got {mock_analyzer.analyze_exception.call_count}"

    @pytest.mark.asyncio
    async def test_smart_retry_llm_says_no_retry(self):
        """Test smart retry when LLM recommends NOT to retry."""
        call_count = 0
        mock_analyzer = AsyncMock()
        mock_analyzer.analyze_exception.return_value = RetryDecision(
            should_retry=False,
            reasoning="Authentication error is permanent",
            confidence=0.95,
            suggested_wait_seconds=1,  # Must be >= 1 per Field validation
        )

        # Use the correct signature: analyzer and max_attempts
        @create_smart_retry_decorator(
            analyzer=mock_analyzer,
            max_attempts=3,
        )
        async def auth_fail():
            nonlocal call_count
            call_count += 1
            raise PermissionError("Invalid credentials")

        with pytest.raises(PermissionError):
            await auth_fail()
        
        # Should fail immediately without retries
        assert call_count == 1
        # Note: Tenacity may call the predicate multiple times to check retry conditions
        assert mock_analyzer.analyze_exception.call_count >= 1, \
            f"Expected at least 1 call, got {mock_analyzer.analyze_exception.call_count}"


class TestRetryManager:
    """Test RetryManager unified interface."""

    def test_retry_manager_standard_mode(self):
        """Test RetryManager in standard mode (no LLM)."""
        manager = RetryManager(
            smart_enabled=False,
            max_attempts=5,
        )

        assert manager.smart_enabled is False
        assert manager.max_attempts == 5
        assert manager.analyzer is None

        # Get decorator
        decorator = manager.get_retry_decorator()
        assert decorator is not None

        # Check stats
        stats = manager.get_stats()
        assert stats["smart_enabled"] is False
        assert stats["max_attempts"] == 5

    def test_retry_manager_smart_mode(self, monkeypatch):
        """Test RetryManager in smart mode (with LLM)."""
        # Set fake API key to avoid OpenAI initialization error
        monkeypatch.setenv("OPENAI_API_KEY", "test-api-key")
        
        manager = RetryManager(
            smart_enabled=True,
            max_attempts=3,
            llm_provider="openai",
            llm_model="gpt-4o-mini",
            llm_api_key="test-api-key",
        )

        assert manager.smart_enabled is True
        assert manager.max_attempts == 3
        assert manager.analyzer is not None

        # Get decorator
        decorator = manager.get_retry_decorator()
        assert decorator is not None

        # Check stats
        stats = manager.get_stats()
        assert stats["smart_enabled"] is True
        assert "analyzer" in stats

    def test_retry_manager_stats_tracking(self):
        """Test retry statistics tracking."""
        manager = RetryManager(smart_enabled=False, max_attempts=3)

        # Get basic stats (no record_attempt method in actual implementation)
        stats = manager.get_stats()
        assert stats["smart_enabled"] is False
        assert stats["max_attempts"] == 3

    @pytest.mark.asyncio
    async def test_retry_manager_decorator_integration(self):
        """Test RetryManager decorator integration."""
        manager = RetryManager(smart_enabled=False, max_attempts=3)
        
        call_count = 0

        @manager.get_retry_decorator()
        async def test_function():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise TimeoutError("Temporary failure")
            return "success"

        result = await test_function()
        assert result == "success"
        assert call_count == 2

        # Check basic stats
        stats = manager.get_stats()
        assert stats["smart_enabled"] is False
        assert stats["max_attempts"] == 3
