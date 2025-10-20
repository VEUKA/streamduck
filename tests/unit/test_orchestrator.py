"""
Unit tests for Pipeline Orchestrator (from src/pipeline/orchestrator.py).

Tests cover:
- PipelineMapping initialization and lifecycle
- Message processing and ingestion coordination
- Multi-mapping orchestration
- Statistics and metrics aggregation
- Health checking and monitoring
- Graceful shutdown and resource cleanup
- Error handling and recovery
"""

import asyncio
from datetime import datetime, timezone
from typing import Any, Dict, List
from unittest.mock import AsyncMock, MagicMock, Mock, call, patch

import pytest

from src.pipeline.orchestrator import PipelineMapping, PipelineOrchestrator
from src.utils.config import (
    EventHubConfig,
    EventHubMotherDuckMapping,
    MotherDuckConfig,
    StreamDuckConfig,
)


# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture
def sample_eventhub_config() -> EventHubConfig:
    """Create a sample EventHub configuration."""
    return EventHubConfig(
        namespace="test.servicebus.windows.net",
        name="test-hub",
        consumer_group="test-group",
        connection_string="Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test",
    )


@pytest.fixture
def sample_motherduck_config() -> MotherDuckConfig:
    """Create a sample MotherDuck configuration."""
    return MotherDuckConfig(
        database="test_database",
        schema_name="test_schema",
        table_name="test_table",
        token="test_token_12345",
        batch_size=1000,
        max_retry_attempts=3,
        retry_delay_seconds=5,
        connection_timeout_seconds=30,
    )


@pytest.fixture
def sample_mapping_config() -> EventHubMotherDuckMapping:
    """Create a sample EventHub->MotherDuck mapping configuration."""
    return EventHubMotherDuckMapping(
        event_hub_key="TEST_EVENTHUB",
        motherduck_key="TEST_MOTHERDUCK",
    )


@pytest.fixture
def sample_stream_duck_config(
    sample_eventhub_config, sample_motherduck_config, sample_mapping_config
) -> StreamDuckConfig:
    """Create a sample StreamDuck configuration."""
    config = MagicMock(spec=StreamDuckConfig)
    config.mappings = [sample_mapping_config]
    config.get_event_hub_config = MagicMock(return_value=sample_eventhub_config)
    config.get_motherduck_config = MagicMock(return_value=sample_motherduck_config)
    return config


@pytest.fixture
def mock_motherduck_client():
    """Create a mock MotherDuck streaming client."""
    mock_client = MagicMock()
    mock_client.start = MagicMock()
    mock_client.stop = MagicMock()
    mock_client.ingest_batch = MagicMock(return_value=True)
    mock_client.get_stats = MagicMock(return_value={"total_messages_sent": 0})
    mock_client.health_check = MagicMock(
        return_value={"client_status": "running", "connection_active": True, "errors": []}
    )
    return mock_client


@pytest.fixture
def mock_eventhub_consumer():
    """Create a mock EventHub async consumer."""
    mock_consumer = MagicMock()
    mock_consumer.start = AsyncMock()
    mock_consumer.stop = AsyncMock()
    mock_consumer.get_stats = MagicMock(return_value={"start_time": "2025-10-17T12:00:00Z"})
    return mock_consumer


# ============================================================================
# PipelineMapping Tests
# ============================================================================


class TestPipelineMappingInitialization:
    """Test PipelineMapping initialization."""

    def test_init_with_valid_config(self, sample_mapping_config, sample_stream_duck_config):
        """Test initializing mapping with valid configuration."""
        mapping = PipelineMapping(
            mapping_config=sample_mapping_config,
            pipeline_config=sample_stream_duck_config,
        )

        assert mapping.mapping_config == sample_mapping_config
        assert mapping.pipeline_config == sample_stream_duck_config
        assert mapping.eventhub_consumer is None
        assert mapping.motherduck_client is None
        assert mapping.running is False

    def test_init_stats_structure(self, sample_mapping_config, sample_stream_duck_config):
        """Test that stats are properly initialized."""
        mapping = PipelineMapping(
            mapping_config=sample_mapping_config,
            pipeline_config=sample_stream_duck_config,
        )

        assert "mapping_key" in mapping.stats
        assert "started_at" in mapping.stats
        assert "messages_processed" in mapping.stats
        assert "batches_processed" in mapping.stats
        assert "last_activity" in mapping.stats
        assert "errors" in mapping.stats
        assert mapping.stats["messages_processed"] == 0
        assert mapping.stats["batches_processed"] == 0

    def test_init_invalid_config(self, sample_mapping_config, sample_stream_duck_config):
        """Test initialization with invalid configuration."""
        sample_stream_duck_config.get_event_hub_config = MagicMock(return_value=None)

        with pytest.raises(ValueError, match="Invalid mapping configuration"):
            PipelineMapping(
                mapping_config=sample_mapping_config,
                pipeline_config=sample_stream_duck_config,
            )


class TestPipelineMappingLifecycle:
    """Test PipelineMapping lifecycle management."""

    @patch("src.pipeline.orchestrator.create_motherduck_streaming_client")
    @patch("src.pipeline.orchestrator.EventHubAsyncConsumer")
    def test_start_initializes_components(
        self,
        mock_consumer_class,
        mock_create_client,
        sample_mapping_config,
        sample_stream_duck_config,
        mock_motherduck_client,
        mock_eventhub_consumer,
    ):
        """Test that start() initializes both components."""
        mock_create_client.return_value = mock_motherduck_client
        mock_consumer_class.return_value = mock_eventhub_consumer

        mapping = PipelineMapping(
            mapping_config=sample_mapping_config,
            pipeline_config=sample_stream_duck_config,
        )
        mapping.start()

        assert mapping.running is True
        assert mapping.motherduck_client is not None
        assert mapping.eventhub_consumer is not None
        mock_motherduck_client.start.assert_called_once()

    @patch("src.pipeline.orchestrator.create_motherduck_streaming_client")
    @patch("src.pipeline.orchestrator.EventHubAsyncConsumer")
    def test_start_sets_timestamp(
        self,
        mock_consumer_class,
        mock_create_client,
        sample_mapping_config,
        sample_stream_duck_config,
        mock_motherduck_client,
        mock_eventhub_consumer,
    ):
        """Test that start() records start timestamp."""
        mock_create_client.return_value = mock_motherduck_client
        mock_consumer_class.return_value = mock_eventhub_consumer

        mapping = PipelineMapping(
            mapping_config=sample_mapping_config,
            pipeline_config=sample_stream_duck_config,
        )
        before = datetime.now(timezone.utc)
        mapping.start()
        after = datetime.now(timezone.utc)

        assert mapping.stats["started_at"] is not None
        assert before <= mapping.stats["started_at"] <= after

    def test_start_idempotent(self, sample_mapping_config, sample_stream_duck_config):
        """Test that start() doesn't reinitialize if already running."""
        with patch("src.pipeline.orchestrator.create_motherduck_streaming_client") as mock_create:
            with patch("src.pipeline.orchestrator.EventHubAsyncConsumer"):
                mapping = PipelineMapping(
                    mapping_config=sample_mapping_config,
                    pipeline_config=sample_stream_duck_config,
                )
                mapping.start()
                first_call_count = mock_create.call_count

                mapping.start()
                # Should not create new client on second start
                assert mock_create.call_count == first_call_count

    @pytest.mark.asyncio
    async def test_stop_closes_components(
        self, sample_mapping_config, sample_stream_duck_config
    ):
        """Test that stop() closes components gracefully."""
        with patch("src.pipeline.orchestrator.create_motherduck_streaming_client") as mock_create:
            with patch("src.pipeline.orchestrator.EventHubAsyncConsumer"):
                mock_client = MagicMock()
                mock_client.start = MagicMock()
                mock_client.stop = MagicMock()
                mock_create.return_value = mock_client

                mock_consumer = AsyncMock()
                mock_consumer.start = AsyncMock()
                mock_consumer.stop = AsyncMock()

                mapping = PipelineMapping(
                    mapping_config=sample_mapping_config,
                    pipeline_config=sample_stream_duck_config,
                )
                mapping.start()
                mapping.eventhub_consumer = mock_consumer

                await mapping.stop()

                assert mapping.running is False
                assert mapping.motherduck_client is None
                assert mapping.eventhub_consumer is None
                mock_client.stop.assert_called_once()
                mock_consumer.stop.assert_called_once()

    @pytest.mark.asyncio
    async def test_stop_without_components(self, sample_mapping_config, sample_stream_duck_config):
        """Test that stop() handles missing components gracefully."""
        mapping = PipelineMapping(
            mapping_config=sample_mapping_config,
            pipeline_config=sample_stream_duck_config,
        )
        mapping.running = False

        await mapping.stop()  # Should not raise


# ============================================================================
# Message Processing Tests
# ============================================================================


class TestMessageProcessing:
    """Test message processing in PipelineMapping."""

    def test_process_messages_success(
        self, sample_mapping_config, sample_stream_duck_config, mock_motherduck_client
    ):
        """Test successful message processing."""
        mapping = PipelineMapping(
            mapping_config=sample_mapping_config,
            pipeline_config=sample_stream_duck_config,
        )
        mapping.motherduck_client = mock_motherduck_client

        # Create mock messages
        mock_messages = [MagicMock() for _ in range(3)]
        for msg in mock_messages:
            msg.to_dict = MagicMock(return_value={"event_body": "test"})

        result = mapping._process_messages(mock_messages)

        assert result is True
        assert mapping.stats["messages_processed"] == 3
        assert mapping.stats["batches_processed"] == 1
        assert mapping.stats["last_activity"] is not None

    def test_process_messages_no_client(self, sample_mapping_config, sample_stream_duck_config):
        """Test message processing without MotherDuck client."""
        mapping = PipelineMapping(
            mapping_config=sample_mapping_config,
            pipeline_config=sample_stream_duck_config,
        )
        mapping.motherduck_client = None
        mock_messages = [MagicMock()]

        result = mapping._process_messages(mock_messages)

        assert result is False
        assert mapping.stats["messages_processed"] == 0

    def test_process_messages_ingestion_failure(
        self, sample_mapping_config, sample_stream_duck_config
    ):
        """Test message processing when ingestion fails."""
        mock_client = MagicMock()
        mock_client.ingest_batch = MagicMock(return_value=False)

        mapping = PipelineMapping(
            mapping_config=sample_mapping_config,
            pipeline_config=sample_stream_duck_config,
        )
        mapping.motherduck_client = mock_client
        mapping.eventhub_config = MagicMock()

        mock_messages = [MagicMock()]
        for msg in mock_messages:
            msg.to_dict = MagicMock(return_value={"event_body": "test"})

        result = mapping._process_messages(mock_messages)

        assert result is False
        assert mapping.stats["messages_processed"] == 0

    def test_process_messages_exception_handling(
        self, sample_mapping_config, sample_stream_duck_config, mock_motherduck_client
    ):
        """Test message processing with exception."""
        mock_motherduck_client.ingest_batch = MagicMock(side_effect=Exception("DB Error"))

        mapping = PipelineMapping(
            mapping_config=sample_mapping_config,
            pipeline_config=sample_stream_duck_config,
        )
        mapping.motherduck_client = mock_motherduck_client
        mapping.eventhub_config = MagicMock()

        mock_messages = [MagicMock()]
        for msg in mock_messages:
            msg.to_dict = MagicMock(return_value={"event_body": "test"})

        result = mapping._process_messages(mock_messages)

        assert result is False
        assert len(mapping.stats["errors"]) > 0


# ============================================================================
# Statistics and Monitoring Tests
# ============================================================================


class TestPipelineMappingStatistics:
    """Test statistics collection in PipelineMapping."""

    def test_get_stats_basic(self, sample_mapping_config, sample_stream_duck_config):
        """Test getting basic stats."""
        mapping = PipelineMapping(
            mapping_config=sample_mapping_config,
            pipeline_config=sample_stream_duck_config,
        )

        stats = mapping.get_stats()

        assert "mapping_key" in stats
        assert "messages_processed" in stats
        assert "batches_processed" in stats

    def test_get_stats_with_components(
        self,
        sample_mapping_config,
        sample_stream_duck_config,
        mock_motherduck_client,
        mock_eventhub_consumer,
    ):
        """Test getting stats with active components."""
        mapping = PipelineMapping(
            mapping_config=sample_mapping_config,
            pipeline_config=sample_stream_duck_config,
        )
        mapping.motherduck_client = mock_motherduck_client
        mapping.eventhub_consumer = mock_eventhub_consumer

        stats = mapping.get_stats()

        assert "motherduck" in stats
        assert "eventhub" in stats

    def test_get_stats_runtime_calculation(self, sample_mapping_config, sample_stream_duck_config):
        """Test runtime calculation in stats."""
        mapping = PipelineMapping(
            mapping_config=sample_mapping_config,
            pipeline_config=sample_stream_duck_config,
        )
        mapping.stats["started_at"] = datetime.now(timezone.utc)

        stats = mapping.get_stats()

        assert "runtime_seconds" in stats
        assert stats["runtime_seconds"] >= 0


class TestPipelineMappingHealth:
    """Test health checking in PipelineMapping."""

    def test_health_check_not_running(self, sample_mapping_config, sample_stream_duck_config):
        """Test health check when mapping is not running."""
        mapping = PipelineMapping(
            mapping_config=sample_mapping_config,
            pipeline_config=sample_stream_duck_config,
        )

        health = mapping.health_check()

        assert health["overall_status"] == "stopped"

    def test_health_check_running(
        self,
        sample_mapping_config,
        sample_stream_duck_config,
        mock_motherduck_client,
        mock_eventhub_consumer,
    ):
        """Test health check when mapping is running."""
        mapping = PipelineMapping(
            mapping_config=sample_mapping_config,
            pipeline_config=sample_stream_duck_config,
        )
        mapping.running = True
        mapping.motherduck_client = mock_motherduck_client
        mapping.eventhub_consumer = mock_eventhub_consumer
        mock_eventhub_consumer.get_stats = MagicMock(return_value={"start_time": "2025-10-17"})

        health = mapping.health_check()

        assert health["overall_status"] == "running"
        assert health["eventhub_status"] == "running"
        assert health["motherduck_status"] == "running"


# ============================================================================
# PipelineOrchestrator Tests
# ============================================================================


class TestPipelineOrchestratorInitialization:
    """Test PipelineOrchestrator initialization."""

    def test_init_basic(self, sample_stream_duck_config):
        """Test orchestrator initialization."""
        orchestrator = PipelineOrchestrator(sample_stream_duck_config)

        assert orchestrator.config == sample_stream_duck_config
        assert orchestrator.mappings == []
        assert orchestrator.running is False
        assert orchestrator.start_time is None

    def test_init_stats_structure(self, sample_stream_duck_config):
        """Test stats initialization."""
        orchestrator = PipelineOrchestrator(sample_stream_duck_config)

        assert "total_mappings" in orchestrator.stats
        assert "active_mappings" in orchestrator.stats
        assert "total_messages_processed" in orchestrator.stats
        assert "total_batches_processed" in orchestrator.stats
        assert "errors" in orchestrator.stats


class TestPipelineOrchestratorInitialize:
    """Test orchestrator initialization method."""

    def test_initialize_creates_mappings(self, sample_stream_duck_config):
        """Test that initialize() creates mappings."""
        with patch("src.pipeline.orchestrator.PipelineMapping") as mock_mapping_class:
            mock_mapping = MagicMock()
            mock_mapping_class.return_value = mock_mapping

            orchestrator = PipelineOrchestrator(sample_stream_duck_config)
            orchestrator.initialize()

            assert orchestrator.stats["total_mappings"] == 1
            assert len(orchestrator.mappings) == 1

    def test_initialize_multiple_mappings(self, sample_stream_duck_config):
        """Test initialization with multiple mappings."""
        sample_stream_duck_config.mappings = [
            MagicMock(event_hub_key="TEST_HUB1", motherduck_key="TEST_MD1"),
            MagicMock(event_hub_key="TEST_HUB2", motherduck_key="TEST_MD2"),
        ]

        with patch("src.pipeline.orchestrator.PipelineMapping") as mock_mapping_class:
            mock_mapping = MagicMock()
            mock_mapping_class.return_value = mock_mapping

            orchestrator = PipelineOrchestrator(sample_stream_duck_config)
            orchestrator.initialize()

            assert orchestrator.stats["total_mappings"] == 2
            assert len(orchestrator.mappings) == 2


class TestPipelineOrchestratorLifecycle:
    """Test orchestrator lifecycle."""

    def test_start_starts_all_mappings(self, sample_stream_duck_config):
        """Test that start() starts all mappings."""
        with patch("src.pipeline.orchestrator.PipelineMapping") as mock_mapping_class:
            mock_mapping = MagicMock()
            mock_mapping.start = MagicMock()
            mock_mapping_class.return_value = mock_mapping

            orchestrator = PipelineOrchestrator(sample_stream_duck_config)
            orchestrator.initialize()
            orchestrator.start()

            assert orchestrator.running is True
            mock_mapping.start.assert_called()

    def test_start_sets_timestamp(self, sample_stream_duck_config):
        """Test that start() records timestamp."""
        with patch("src.pipeline.orchestrator.PipelineMapping"):
            orchestrator = PipelineOrchestrator(sample_stream_duck_config)
            orchestrator.initialize()

            before = datetime.now(timezone.utc)
            orchestrator.start()
            after = datetime.now(timezone.utc)

            assert orchestrator.start_time is not None
            assert before <= orchestrator.start_time <= after

    def test_start_idempotent(self, sample_stream_duck_config):
        """Test that start() doesn't reinitialize if already running."""
        with patch("src.pipeline.orchestrator.PipelineMapping") as mock_mapping_class:
            mock_mapping = MagicMock()
            mock_mapping.start = MagicMock()
            mock_mapping_class.return_value = mock_mapping

            orchestrator = PipelineOrchestrator(sample_stream_duck_config)
            orchestrator.initialize()
            orchestrator.start()
            first_call_count = mock_mapping.start.call_count

            orchestrator.start()

            # Should not call start again on second start
            assert mock_mapping.start.call_count == first_call_count

    @pytest.mark.asyncio
    async def test_stop_stops_all_mappings(self, sample_stream_duck_config):
        """Test that stop() stops all mappings."""
        with patch("src.pipeline.orchestrator.PipelineMapping") as mock_mapping_class:
            mock_mapping = MagicMock()
            mock_mapping.stop = AsyncMock()
            mock_mapping_class.return_value = mock_mapping

            orchestrator = PipelineOrchestrator(sample_stream_duck_config)
            orchestrator.initialize()
            orchestrator.running = True

            await orchestrator.stop()

            assert orchestrator.running is False
            mock_mapping.stop.assert_called()


# ============================================================================
# Orchestrator Statistics Tests
# ============================================================================


class TestOrchestratorStatistics:
    """Test statistics aggregation in orchestrator."""

    def test_get_stats_basic(self, sample_stream_duck_config):
        """Test getting orchestrator stats."""
        with patch("src.pipeline.orchestrator.PipelineMapping") as mock_mapping_class:
            mock_mapping = MagicMock()
            mock_mapping.get_stats = MagicMock(
                return_value={
                    "messages_processed": 10,
                    "batches_processed": 2,
                }
            )
            mock_mapping_class.return_value = mock_mapping

            orchestrator = PipelineOrchestrator(sample_stream_duck_config)
            orchestrator.initialize()

            stats = orchestrator.get_stats()

            assert "total_messages_processed" in stats
            assert "total_batches_processed" in stats

    def test_get_stats_aggregates_mappings(self, sample_stream_duck_config):
        """Test that stats aggregate from all mappings."""
        sample_stream_duck_config.mappings = [
            MagicMock(event_hub_key="TEST_HUB1", motherduck_key="TEST_MD1"),
            MagicMock(event_hub_key="TEST_HUB2", motherduck_key="TEST_MD2"),
        ]

        with patch("src.pipeline.orchestrator.PipelineMapping") as mock_mapping_class:
            mock_mapping = MagicMock()
            mock_mapping.get_stats = MagicMock(
                return_value={
                    "messages_processed": 10,
                    "batches_processed": 2,
                }
            )
            mock_mapping_class.return_value = mock_mapping

            orchestrator = PipelineOrchestrator(sample_stream_duck_config)
            orchestrator.initialize()
            orchestrator.start_time = datetime.now(timezone.utc)

            stats = orchestrator.get_stats()

            assert stats["total_messages_processed"] == 20  # 10 per mapping
            assert stats["total_batches_processed"] == 4  # 2 per mapping
            assert len(stats["mappings"]) == 2

    def test_get_stats_throughput_calculation(self, sample_stream_duck_config):
        """Test throughput calculation in stats."""
        with patch("src.pipeline.orchestrator.PipelineMapping") as mock_mapping_class:
            mock_mapping = MagicMock()
            mock_mapping.get_stats = MagicMock(
                return_value={
                    "messages_processed": 100,
                    "batches_processed": 10,
                }
            )
            mock_mapping_class.return_value = mock_mapping

            orchestrator = PipelineOrchestrator(sample_stream_duck_config)
            orchestrator.initialize()
            orchestrator.start_time = datetime.now(timezone.utc)
            # Fake a runtime
            orchestrator.stats["runtime_seconds"] = 10.0

            stats = orchestrator.get_stats()

            assert "messages_per_second" in stats
            assert "batches_per_second" in stats


# ============================================================================
# Orchestrator Health Check Tests
# ============================================================================


class TestOrchestratorHealth:
    """Test health checking in orchestrator."""

    def test_health_check_not_running(self, sample_stream_duck_config):
        """Test health check when orchestrator is not running."""
        orchestrator = PipelineOrchestrator(sample_stream_duck_config)
        orchestrator.initialize()

        health = orchestrator.health_check()

        assert health["orchestrator_status"] == "stopped"

    def test_health_check_running(self, sample_stream_duck_config):
        """Test health check when orchestrator is running."""
        with patch("src.pipeline.orchestrator.PipelineMapping") as mock_mapping_class:
            mock_mapping = MagicMock()
            mock_mapping.health_check = MagicMock(
                return_value={"overall_status": "running"}
            )
            mock_mapping_class.return_value = mock_mapping

            orchestrator = PipelineOrchestrator(sample_stream_duck_config)
            orchestrator.initialize()
            orchestrator.running = True

            health = orchestrator.health_check()

            assert health["orchestrator_status"] == "running"
            assert health["healthy_mappings"] == 1

    def test_health_check_mixed_status(self, sample_stream_duck_config):
        """Test health check with mixed mapping statuses."""
        sample_stream_duck_config.mappings = [
            MagicMock(event_hub_key="TEST_HUB1", motherduck_key="TEST_MD1"),
            MagicMock(event_hub_key="TEST_HUB2", motherduck_key="TEST_MD2"),
        ]

        with patch("src.pipeline.orchestrator.PipelineMapping") as mock_mapping_class:
            # Create two different mocks with different health statuses
            mock_mapping_running = MagicMock()
            mock_mapping_running.health_check = MagicMock(
                return_value={"overall_status": "running"}
            )

            mock_mapping_stopped = MagicMock()
            mock_mapping_stopped.health_check = MagicMock(
                return_value={"overall_status": "stopped"}
            )

            mock_mapping_class.side_effect = [mock_mapping_running, mock_mapping_stopped]

            orchestrator = PipelineOrchestrator(sample_stream_duck_config)
            orchestrator.initialize()
            orchestrator.running = True

            health = orchestrator.health_check()

            assert health["healthy_mappings"] == 1


# ============================================================================
# Async Operations Tests
# ============================================================================


class TestOrchestratorAsync:
    """Test async operations in orchestrator."""

    @pytest.mark.asyncio
    async def test_run_async_starts_mappings(self, sample_stream_duck_config):
        """Test that run_async starts async components."""
        with patch("src.pipeline.orchestrator.PipelineMapping") as mock_mapping_class:
            mock_mapping = MagicMock()
            mock_mapping.start_async = AsyncMock()
            mock_mapping_class.return_value = mock_mapping

            orchestrator = PipelineOrchestrator(sample_stream_duck_config)
            orchestrator.initialize()
            orchestrator.running = True

            # Start run_async with a timeout to prevent infinite loop
            task = asyncio.create_task(orchestrator.run_async())

            # Give it a moment to create the async tasks
            await asyncio.sleep(0.1)

            # Stop the orchestrator to break the loop
            orchestrator.running = False

            try:
                await asyncio.wait_for(task, timeout=1.0)
            except asyncio.TimeoutError:
                task.cancel()

            mock_mapping.start_async.assert_called()

    @pytest.mark.asyncio
    async def test_stop_cancels_tasks(self, sample_stream_duck_config):
        """Test that stop() properly cancels async tasks."""
        with patch("src.pipeline.orchestrator.PipelineMapping") as mock_mapping_class:
            mock_mapping = MagicMock()
            mock_mapping.stop = AsyncMock()
            mock_mapping_class.return_value = mock_mapping

            orchestrator = PipelineOrchestrator(sample_stream_duck_config)
            orchestrator.initialize()
            orchestrator.running = True

            # Add a fake task
            fake_task = asyncio.create_task(asyncio.sleep(10))
            orchestrator.tasks.add(fake_task)

            await orchestrator.stop()

            assert orchestrator.running is False
            assert len(orchestrator.tasks) == 0


# ============================================================================
# Error Handling Tests
# ============================================================================


class TestOrchestratorErrorHandling:
    """Test error handling in orchestrator."""

    def test_initialize_error_handling(self, sample_stream_duck_config):
        """Test error handling during initialization."""
        sample_stream_duck_config.mappings = [MagicMock()]
        sample_stream_duck_config.get_event_hub_config = MagicMock(return_value=None)

        orchestrator = PipelineOrchestrator(sample_stream_duck_config)

        with pytest.raises(ValueError):
            orchestrator.initialize()

    def test_start_cleanup_on_error(self, sample_stream_duck_config):
        """Test that start() cleans up on error."""
        with patch("src.pipeline.orchestrator.PipelineMapping") as mock_mapping_class:
            mock_mapping = MagicMock()
            mock_mapping.start = MagicMock(side_effect=Exception("Start failed"))
            mock_mapping.motherduck_client = MagicMock()
            mock_mapping_class.return_value = mock_mapping

            orchestrator = PipelineOrchestrator(sample_stream_duck_config)
            orchestrator.initialize()

            with pytest.raises(Exception):
                orchestrator.start()

    @pytest.mark.asyncio
    async def test_run_async_error_handling(self, sample_stream_duck_config):
        """Test error handling in run_async."""
        with patch("src.pipeline.orchestrator.PipelineMapping") as mock_mapping_class:
            mock_mapping = MagicMock()
            mock_mapping.start_async = AsyncMock(side_effect=Exception("Async failed"))
            mock_mapping.stop = AsyncMock()
            mock_mapping_class.return_value = mock_mapping

            orchestrator = PipelineOrchestrator(sample_stream_duck_config)
            orchestrator.initialize()
            orchestrator.running = True

            with pytest.raises(Exception):
                await orchestrator.run_async()

            mock_mapping.stop.assert_called()


# ============================================================================
# Integration Tests
# ============================================================================


class TestOrchestratorIntegration:
    """Test orchestrator integration scenarios."""

    @pytest.mark.asyncio
    async def test_full_lifecycle(self, sample_stream_duck_config):
        """Test complete orchestrator lifecycle."""
        with patch("src.pipeline.orchestrator.PipelineMapping") as mock_mapping_class:
            mock_mapping = MagicMock()
            mock_mapping.start = MagicMock()
            mock_mapping.start_async = AsyncMock()
            mock_mapping.stop = AsyncMock()
            mock_mapping_class.return_value = mock_mapping

            orchestrator = PipelineOrchestrator(sample_stream_duck_config)

            # Initialize
            orchestrator.initialize()
            assert len(orchestrator.mappings) == 1

            # Start
            orchestrator.start()
            assert orchestrator.running is True
            mock_mapping.start.assert_called()

            # Stop
            await orchestrator.stop()
            assert orchestrator.running is False
            mock_mapping.stop.assert_called()

    def test_stats_accumulation(self, sample_stream_duck_config):
        """Test statistics accumulation across operations."""
        with patch("src.pipeline.orchestrator.PipelineMapping") as mock_mapping_class:
            mock_mapping = MagicMock()
            mock_mapping.get_stats = MagicMock(
                return_value={
                    "messages_processed": 50,
                    "batches_processed": 5,
                }
            )
            mock_mapping_class.return_value = mock_mapping

            orchestrator = PipelineOrchestrator(sample_stream_duck_config)
            orchestrator.initialize()
            orchestrator.start()

            stats = orchestrator.get_stats()

            assert stats["total_messages_processed"] == 50
            assert stats["total_batches_processed"] == 5
            assert stats["active_mappings"] == 1
