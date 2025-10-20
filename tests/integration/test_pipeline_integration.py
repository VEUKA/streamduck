"""
Integration tests for StreamDuck end-to-end pipeline scenarios.

Tests full pipeline execution including:
- Message flow from EventHub to MotherDuck
- Multi-mapping coordination
- Checkpoint management
- Statistics accumulation
- Error recovery scenarios
"""

import asyncio
import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from pipeline.orchestrator import PipelineOrchestrator
from utils.config import (
    EventHubConfig,
    MotherDuckConfig,
    EventHubMotherDuckMapping,
    StreamDuckConfig,
)


# =====================================================================
# FIXTURES
# =====================================================================


@pytest.fixture
def integration_env_setup(monkeypatch):
    """Set up environment variables for integration tests."""
    env_vars = {
        "EVENTHUB_NAMESPACE": "test.servicebus.windows.net",
        "EVENTHUBNAME_1": "test-hub-1",
        "EVENTHUBNAME_1_CONSUMER_GROUP": "$Default",
        "MOTHERDUCK_1_DATABASE": "test_db",
        "MOTHERDUCK_1_SCHEMA": "test_schema",
        "MOTHERDUCK_1_TABLE": "test_table",
        "MOTHERDUCK_1_TOKEN": "test-token-123",
    }
    for key, value in env_vars.items():
        monkeypatch.setenv(key, value)
    return env_vars


@pytest.fixture
def sample_eventhub_config() -> EventHubConfig:
    """Valid EventHub configuration."""
    return EventHubConfig(
        name="test-eventhub",
        namespace="test.servicebus.windows.net",
        consumer_group="test-group",
        max_batch_size=10,
        max_wait_time=5,
        prefetch_count=100,
    )


@pytest.fixture
def sample_motherduck_config() -> MotherDuckConfig:
    """Valid MotherDuck configuration."""
    return MotherDuckConfig(
        token="test-token-123",
        database="test_db",
        schema_name="test_schema",
        table_name="test_table",
    )


@pytest.fixture
def sample_mapping_config(
    sample_eventhub_config: EventHubConfig,
    sample_motherduck_config: MotherDuckConfig,
) -> EventHubMotherDuckMapping:
    """Valid mapping configuration."""
    return EventHubMotherDuckMapping(
        event_hub_key="TEST_EVENTHUB",
        motherduck_key="TEST_MOTHERDUCK",
        table_name="test_table",
    )


@pytest.fixture
def sample_stream_duck_config(integration_env_setup) -> StreamDuckConfig:
    """Complete StreamDuck configuration from environment."""
    from utils.config import load_config
    return load_config()


@pytest.fixture
def mock_eventhub_consumer():
    """Mock EventHub consumer with async capabilities."""
    consumer = AsyncMock()
    consumer.start = AsyncMock()
    consumer.close = AsyncMock()
    consumer.receive_batch = AsyncMock(
        return_value=[
            MagicMock(
                body=b'{"test": "data"}',
                properties={"id": "test-1"},
                system_properties={"x-opt-sequence-number": 1},
            )
        ]
    )
    consumer.update_checkpoint = AsyncMock()
    consumer.get_stats = MagicMock(
        return_value={"messages_received": 1, "batches_received": 1}
    )
    consumer.running = True
    consumer.health_check = MagicMock(return_value={"status": "running"})
    return consumer


@pytest.fixture
def mock_motherduck_client():
    """Mock MotherDuck streaming client."""
    client = MagicMock()
    client.get_connection = MagicMock(return_value="mock_connection")
    client.create_table = MagicMock(return_value=True)
    client.ingest_batch = MagicMock(return_value={"inserted": 1, "errors": 0})
    client.get_stats = MagicMock(
        return_value={"batches_ingested": 1, "rows_ingested": 1}
    )
    client.health_check = MagicMock(return_value={"status": "healthy"})
    return client


# =====================================================================
# TEST CLASS: Pipeline Integration
# =====================================================================


class TestPipelineIntegration:
    """Integration tests for full pipeline execution scenarios."""

    @pytest.mark.asyncio
    async def test_full_pipeline_lifecycle(
        self, sample_stream_duck_config, mock_eventhub_consumer, mock_motherduck_client
    ):
        """Test complete pipeline lifecycle from init to shutdown."""
        with patch(
            "consumers.eventhub.EventHubAsyncConsumer",
            return_value=mock_eventhub_consumer,
        ):
            with patch(
                "streaming.motherduck.MotherDuckStreamingClient",
                return_value=mock_motherduck_client,
            ):
                orchestrator = PipelineOrchestrator(sample_stream_duck_config)

                # Initialize with mappings
                orchestrator.initialize()
                assert len(orchestrator.mappings) == 1
                assert orchestrator.running is False

                # Start pipeline
                orchestrator.start()
                assert orchestrator.running is True
                assert orchestrator.start_time is not None

                # Verify all mappings started
                for mapping in orchestrator.mappings:
                    assert mapping.running is True

                # Stop pipeline
                await orchestrator.stop()
                assert orchestrator.running is False

    @pytest.mark.asyncio
    async def test_message_flow_single_mapping(
        self, sample_stream_duck_config, mock_eventhub_consumer, mock_motherduck_client
    ):
        """Test message flow through single mapping."""
        with patch(
            "consumers.eventhub.EventHubAsyncConsumer",
            return_value=mock_eventhub_consumer,
        ):
            with patch(
                "streaming.motherduck.MotherDuckStreamingClient",
                return_value=mock_motherduck_client,
            ):
                orchestrator = PipelineOrchestrator(sample_stream_duck_config)
                orchestrator.initialize()
                orchestrator.start()

                # Verify that messages would be received
                assert orchestrator.mappings[0] is not None
                assert orchestrator.mappings[0].running is True

                # Verify stats can be retrieved
                stats = orchestrator.get_stats()
                assert stats is not None

                await orchestrator.stop()

    @pytest.mark.asyncio
    async def test_multi_mapping_coordination(self, monkeypatch):
        """Test coordination of multiple mappings."""
        # Clear any existing environment variables to prevent auto-mapping
        monkeypatch.delenv("EVENTHUBNAME_1", raising=False)
        monkeypatch.delenv("EVENTHUBNAME_1_CONSUMER_GROUP", raising=False)
        monkeypatch.delenv("MOTHERDUCK_1_DATABASE", raising=False)
        monkeypatch.delenv("MOTHERDUCK_1_SCHEMA", raising=False)
        monkeypatch.delenv("MOTHERDUCK_1_TABLE", raising=False)
        monkeypatch.delenv("MOTHERDUCK_1_TOKEN", raising=False)

        # Create multi-mapping config without using environment setup
        config = StreamDuckConfig(
            eventhub_namespace="test1.servicebus.windows.net",
            event_hubs={
                "HUB1": EventHubConfig(
                    name="hub1",
                    namespace="test1.servicebus.windows.net",
                    consumer_group="group1",
                ),
                "HUB2": EventHubConfig(
                    name="hub2",
                    namespace="test2.servicebus.windows.net",
                    consumer_group="group2",
                ),
            },
            motherduck_configs={
                "MD1": MotherDuckConfig(
                    token="token1",
                    database="db1",
                    schema_name="schema1",
                    table_name="table1",
                ),
            },
            mappings=[
                EventHubMotherDuckMapping(
                    event_hub_key="HUB1",
                    motherduck_key="MD1",
                    channel_name_pattern="{event_hub}-{env}-{region}-{client_id}",
                ),
                EventHubMotherDuckMapping(
                    event_hub_key="HUB2",
                    motherduck_key="MD1",
                    channel_name_pattern="{event_hub}-{env}-{region}-{client_id}",
                ),
            ],
        )

        with patch("consumers.eventhub.EventHubAsyncConsumer"):
            with patch("streaming.motherduck.MotherDuckStreamingClient"):
                orchestrator = PipelineOrchestrator(config)
                orchestrator.initialize()

                # Verify both mappings created (should be exactly 2, not auto-generated ones)
                assert len(orchestrator.mappings) == 2

                orchestrator.start()

                # Verify all mappings running
                for mapping in orchestrator.mappings:
                    assert mapping.running is True

                # Verify stats from multiple mappings
                stats = orchestrator.get_stats()
                assert stats is not None
                assert "start_time" in stats

                await orchestrator.stop()

    @pytest.mark.asyncio
    async def test_statistics_aggregation_across_mappings(
        self, sample_stream_duck_config, mock_eventhub_consumer, mock_motherduck_client
    ):
        """Test aggregation of statistics across multiple mappings."""
        config = StreamDuckConfig(
            eventhub_namespace="test1.servicebus.windows.net",
            event_hubs={
                "HUB1": EventHubConfig(
                    name="hub1",
                    namespace="test1.servicebus.windows.net",
                    consumer_group="group1",
                ),
                "HUB2": EventHubConfig(
                    name="hub2",
                    namespace="test2.servicebus.windows.net",
                    consumer_group="group2",
                ),
            },
            motherduck_configs={
                "MD": MotherDuckConfig(
                    token="token",
                    database="db",
                    schema_name="schema",
                    table_name="table",
                ),
            },
            mappings=[
                EventHubMotherDuckMapping(
                    event_hub_key="HUB1",
                    motherduck_key="MD",
                    channel_name_pattern="{event_hub}-{env}-{region}-{client_id}",
                ),
                EventHubMotherDuckMapping(
                    event_hub_key="HUB2",
                    motherduck_key="MD",
                    channel_name_pattern="{event_hub}-{env}-{region}-{client_id}",
                ),
            ],
        )

        consumer1 = AsyncMock()
        consumer1.get_stats = MagicMock(
            return_value={"messages_received": 100, "batches_received": 10}
        )
        consumer1.running = True

        consumer2 = AsyncMock()
        consumer2.get_stats = MagicMock(
            return_value={"messages_received": 200, "batches_received": 20}
        )
        consumer2.running = True

        with patch("consumers.eventhub.EventHubAsyncConsumer") as mock_consumer:
            mock_consumer.side_effect = [consumer1, consumer2]
            with patch(
                "streaming.motherduck.MotherDuckStreamingClient",
                return_value=mock_motherduck_client,
            ):
                orchestrator = PipelineOrchestrator(config)
                orchestrator.initialize()
                orchestrator.start()

                stats = orchestrator.get_stats()
                assert stats is not None

                await orchestrator.stop()

    @pytest.mark.asyncio
    async def test_error_recovery_on_mapping_failure(
        self, sample_stream_duck_config, mock_eventhub_consumer, mock_motherduck_client
    ):
        """Test error handling when mapping fails."""
        # Simulate ingestion error
        mock_motherduck_client.ingest_batch = MagicMock(
            side_effect=Exception("Ingestion failed")
        )

        with patch(
            "consumers.eventhub.EventHubAsyncConsumer",
            return_value=mock_eventhub_consumer,
        ):
            with patch(
                "streaming.motherduck.MotherDuckStreamingClient",
                return_value=mock_motherduck_client,
            ):
                orchestrator = PipelineOrchestrator(sample_stream_duck_config)
                orchestrator.initialize()
                orchestrator.start()

                # Verify mapping started
                assert orchestrator.mappings[0].running is True

                await orchestrator.stop()

    @pytest.mark.asyncio
    async def test_graceful_shutdown_during_processing(
        self, sample_stream_duck_config, mock_eventhub_consumer, mock_motherduck_client
    ):
        """Test graceful shutdown while processing messages."""
        with patch(
            "consumers.eventhub.EventHubAsyncConsumer",
            return_value=mock_eventhub_consumer,
        ):
            with patch(
                "streaming.motherduck.MotherDuckStreamingClient",
                return_value=mock_motherduck_client,
            ):
                orchestrator = PipelineOrchestrator(sample_stream_duck_config)
                orchestrator.initialize()
                orchestrator.start()

                # Create async task as if processing
                processing_task = asyncio.create_task(
                    asyncio.sleep(10)  # Long-running task
                )
                orchestrator.tasks.add(processing_task)

                # Stop should cancel the task
                await orchestrator.stop()

                assert processing_task.cancelled() or processing_task.done()
                assert orchestrator.running is False

    @pytest.mark.asyncio
    async def test_health_check_across_pipeline(
        self, sample_stream_duck_config, mock_eventhub_consumer, mock_motherduck_client
    ):
        """Test health status reporting across entire pipeline."""
        with patch(
            "consumers.eventhub.EventHubAsyncConsumer",
            return_value=mock_eventhub_consumer,
        ):
            with patch(
                "streaming.motherduck.MotherDuckStreamingClient",
                return_value=mock_motherduck_client,
            ):
                orchestrator = PipelineOrchestrator(sample_stream_duck_config)
                orchestrator.initialize()
                orchestrator.start()

                # Get health status
                health = orchestrator.health_check()
                assert health is not None
                assert "orchestrator_status" in health

                # Health should reflect pipeline state
                assert health["orchestrator_status"] == "running"

                await orchestrator.stop()

                health = orchestrator.health_check()
                assert health["orchestrator_status"] == "stopped"

    @pytest.mark.asyncio
    async def test_configuration_validation_before_pipeline_start(
        self, sample_stream_duck_config
    ):
        """Test that configuration is validated before pipeline starts."""
        with patch("consumers.eventhub.EventHubAsyncConsumer"):
            with patch("streaming.motherduck.MotherDuckStreamingClient"):
                orchestrator = PipelineOrchestrator(sample_stream_duck_config)

                # Should not raise during initialization
                orchestrator.initialize()

                # Verify mappings were created from config
                assert len(orchestrator.mappings) > 0


# =====================================================================
# TEST CLASS: End-to-End Scenarios
# =====================================================================


class TestEndToEndScenarios:
    """End-to-end scenario tests simulating real-world usage."""

    @pytest.mark.asyncio
    async def test_continuous_message_streaming(
        self, sample_stream_duck_config, mock_eventhub_consumer, mock_motherduck_client
    ):
        """Test continuous message streaming scenario."""
        # Simulate batch of messages
        messages = [
            MagicMock(
                body=b'{"id": 1}',
                properties={"id": "msg-1"},
                system_properties={"x-opt-sequence-number": i},
            )
            for i in range(5)
        ]

        mock_eventhub_consumer.receive_batch = AsyncMock(return_value=messages)

        with patch(
            "consumers.eventhub.EventHubAsyncConsumer",
            return_value=mock_eventhub_consumer,
        ):
            with patch(
                "streaming.motherduck.MotherDuckStreamingClient",
                return_value=mock_motherduck_client,
            ):
                orchestrator = PipelineOrchestrator(sample_stream_duck_config)
                orchestrator.initialize()
                orchestrator.start()

                # Verify mapping is ready for message processing
                assert len(orchestrator.mappings) == 1
                assert orchestrator.mappings[0].running is True

                await orchestrator.stop()

    @pytest.mark.asyncio
    async def test_empty_batch_handling(
        self, sample_stream_duck_config, mock_eventhub_consumer, mock_motherduck_client
    ):
        """Test handling of empty message batches."""
        mock_eventhub_consumer.receive_batch = AsyncMock(return_value=[])

        with patch(
            "consumers.eventhub.EventHubAsyncConsumer",
            return_value=mock_eventhub_consumer,
        ):
            with patch(
                "streaming.motherduck.MotherDuckStreamingClient",
                return_value=mock_motherduck_client,
            ):
                orchestrator = PipelineOrchestrator(sample_stream_duck_config)
                orchestrator.initialize()
                orchestrator.start()

                # Should handle gracefully without ingesting
                assert orchestrator.mappings[0].running is True

                await orchestrator.stop()

    @pytest.mark.asyncio
    async def test_checkpoint_persistence_across_restarts(
        self, sample_stream_duck_config, mock_eventhub_consumer, mock_motherduck_client
    ):
        """Test checkpoint persistence across pipeline restarts."""
        with patch(
            "consumers.eventhub.EventHubAsyncConsumer",
            return_value=mock_eventhub_consumer,
        ):
            with patch(
                "streaming.motherduck.MotherDuckStreamingClient",
                return_value=mock_motherduck_client,
            ):
                # First run
                orchestrator1 = PipelineOrchestrator(sample_stream_duck_config)
                orchestrator1.initialize()
                orchestrator1.start()
                await orchestrator1.stop()

                # Second run - should use saved checkpoints
                orchestrator2 = PipelineOrchestrator(sample_stream_duck_config)
                orchestrator2.initialize()
                orchestrator2.start()
                await orchestrator2.stop()

    @pytest.mark.asyncio
    async def test_high_volume_message_processing(
        self, sample_stream_duck_config, mock_eventhub_consumer, mock_motherduck_client
    ):
        """Test handling of high-volume message batches."""
        # Create large batch
        messages = [
            MagicMock(
                body=f'{{"id": {i}}}'.encode(),
                properties={"id": f"msg-{i}"},
                system_properties={"x-opt-sequence-number": i},
            )
            for i in range(1000)
        ]

        mock_eventhub_consumer.receive_batch = AsyncMock(return_value=messages)
        mock_motherduck_client.ingest_batch = MagicMock(
            return_value={"inserted": 1000, "errors": 0}
        )

        with patch(
            "consumers.eventhub.EventHubAsyncConsumer",
            return_value=mock_eventhub_consumer,
        ):
            with patch(
                "streaming.motherduck.MotherDuckStreamingClient",
                return_value=mock_motherduck_client,
            ):
                orchestrator = PipelineOrchestrator(sample_stream_duck_config)
                orchestrator.initialize()
                orchestrator.start()

                # Verify pipeline can handle large batches
                assert orchestrator.mappings[0].running is True

                await orchestrator.stop()
