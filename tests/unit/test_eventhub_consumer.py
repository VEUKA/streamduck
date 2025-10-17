"""
Unit tests for EventHubAsyncConsumer (from src/consumers/eventhub.py).

Tests cover consumer initialization, connection, checkpoint management, 
message processing, batch configuration, and error handling.
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.consumers.eventhub import (
    EventHubAsyncConsumer,
    MotherDuckCheckpointManager,
    PartitionContext,
    EventData,
)
from src.utils.config import EventHubConfig
from src.utils.motherduck import MotherDuckConnectionConfig


# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture
def mock_message_processor():
    """Create a mock message processor callback."""
    return MagicMock(return_value=True)


@pytest.fixture
def eventhub_consumer(
    sample_eventhub_config: EventHubConfig,
    mock_message_processor,
):
    """Create an EventHub consumer instance for testing."""
    return EventHubAsyncConsumer(
        eventhub_config=sample_eventhub_config,
        target_db="mother_ducklake",
        target_schema="ingest",
        target_table="ingestion_data",
        message_processor=mock_message_processor,
        motherduck_config=None,
        batch_size=1000,
        batch_timeout_seconds=300,
    )


@pytest.fixture
def mock_consumer_client():
    """Create a mock EventHub consumer client."""
    mock = MagicMock()
    mock.receive_batch = AsyncMock()
    mock.get_partition_ids = MagicMock(return_value=["0", "1", "2"])
    mock.create_batch_receiver = MagicMock()
    mock.close = AsyncMock()
    return mock


# ============================================================================
# Initialization Tests
# ============================================================================


class TestEventHubConsumerInitialization:
    """Test EventHub consumer initialization."""

    def test_init_with_valid_config(
        self,
        sample_eventhub_config: EventHubConfig,
        mock_message_processor,
    ):
        """Test initializing consumer with valid configuration."""
        consumer = EventHubAsyncConsumer(
            eventhub_config=sample_eventhub_config,
            target_db="test_db",
            target_schema="test_schema",
            target_table="test_table",
            message_processor=mock_message_processor,
            batch_size=2000,
            batch_timeout_seconds=600,
        )

        assert consumer.eventhub_config == sample_eventhub_config
        assert consumer.target_db == "test_db"
        assert consumer.target_schema == "test_schema"
        assert consumer.target_table == "test_table"
        assert consumer.message_processor == mock_message_processor
        assert consumer.batch_size == 2000
        assert consumer.batch_timeout_seconds == 600

    def test_init_with_default_batch_settings(
        self,
        sample_eventhub_config: EventHubConfig,
        mock_message_processor,
    ):
        """Test initialization with default batch settings."""
        consumer = EventHubAsyncConsumer(
            eventhub_config=sample_eventhub_config,
            target_db="db",
            target_schema="schema",
            target_table="table",
            message_processor=mock_message_processor,
        )

        assert consumer.batch_size == 1000
        assert consumer.batch_timeout_seconds == 300

    def test_consumer_initial_state(self, eventhub_consumer):
        """Test consumer has correct initial state."""
        assert eventhub_consumer.client is None
        assert eventhub_consumer.running is False
        assert eventhub_consumer.checkpoint_manager is None


# ============================================================================
# Connection Tests
# ============================================================================


class TestEventHubConsumerConnection:
    """Test consumer connection setup."""

    def test_client_initially_none(self, eventhub_consumer):
        """Test that client is initially None."""
        assert eventhub_consumer.client is None

    def test_checkpoint_manager_initially_none(self, eventhub_consumer):
        """Test that checkpoint_manager is initially None."""
        assert eventhub_consumer.checkpoint_manager is None

    @pytest.mark.asyncio
    async def test_stop_closes_existing_client(self, eventhub_consumer, mock_consumer_client):
        """Test that stop() closes an existing client."""
        eventhub_consumer.client = mock_consumer_client
        eventhub_consumer.running = True

        await eventhub_consumer.stop()

        mock_consumer_client.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_stop_clears_tasks(self, eventhub_consumer):
        """Test that stop() cancels background tasks."""
        # Create a simple coroutine to add as task
        async def dummy_task():
            await asyncio.sleep(100)
        
        eventhub_consumer.running = True
        task = asyncio.create_task(dummy_task())
        eventhub_consumer.tasks.add(task)

        await eventhub_consumer.stop()

        assert len(eventhub_consumer.tasks) == 0


# ============================================================================
# Checkpoint Management Tests
# ============================================================================


class TestEventHubConsumerCheckpointManagement:
    """Test checkpoint management within consumer."""

    def test_checkpoint_manager_attribute_exists(self, eventhub_consumer):
        """Test that consumer has checkpoint_manager attribute."""
        assert hasattr(eventhub_consumer, "checkpoint_manager")

    def test_checkpoint_manager_initially_none(self, eventhub_consumer):
        """Test that checkpoint_manager starts as None."""
        assert eventhub_consumer.checkpoint_manager is None


# ============================================================================
# Configuration Tests
# ============================================================================


class TestEventHubConsumerConfiguration:
    """Test consumer configuration."""

    def test_consumer_preserves_target_paths(
        self,
        sample_eventhub_config: EventHubConfig,
        mock_message_processor,
    ):
        """Test consumer preserves target database paths."""
        consumer = EventHubAsyncConsumer(
            eventhub_config=sample_eventhub_config,
            target_db="prod_db",
            target_schema="audit",
            target_table="events",
            message_processor=mock_message_processor,
        )

        assert consumer.target_db == "prod_db"
        assert consumer.target_schema == "audit"
        assert consumer.target_table == "events"

    def test_consumer_batch_size_configuration(
        self,
        sample_eventhub_config: EventHubConfig,
        mock_message_processor,
    ):
        """Test batch size is configurable."""
        consumer = EventHubAsyncConsumer(
            eventhub_config=sample_eventhub_config,
            target_db="db",
            target_schema="schema",
            target_table="table",
            message_processor=mock_message_processor,
            batch_size=500,
        )

        assert consumer.batch_size == 500

    def test_consumer_batch_timeout_configuration(
        self,
        sample_eventhub_config: EventHubConfig,
        mock_message_processor,
    ):
        """Test batch timeout is configurable."""
        consumer = EventHubAsyncConsumer(
            eventhub_config=sample_eventhub_config,
            target_db="db",
            target_schema="schema",
            target_table="table",
            message_processor=mock_message_processor,
            batch_timeout_seconds=120,
        )

        assert consumer.batch_timeout_seconds == 120


# ============================================================================
# State Tests
# ============================================================================


class TestEventHubConsumerState:
    """Test consumer lifecycle and state management."""

    def test_consumer_initial_running_state(self, eventhub_consumer):
        """Test consumer starts in non-running state."""
        assert eventhub_consumer.running is False

    def test_consumer_has_tasks_set(self, eventhub_consumer):
        """Test consumer maintains a set of tasks."""
        assert hasattr(eventhub_consumer, "tasks")
        assert isinstance(eventhub_consumer.tasks, set)
        assert len(eventhub_consumer.tasks) == 0

    def test_consumer_has_batch_member(self, eventhub_consumer):
        """Test consumer maintains current batch."""
        assert hasattr(eventhub_consumer, "current_batch")
        assert eventhub_consumer.current_batch is None


# ============================================================================
# Error Handling Tests
# ============================================================================


class TestEventHubConsumerErrorHandling:
    """Test error handling in consumer."""

    @pytest.mark.asyncio
    async def test_stop_without_running_flag(self, eventhub_consumer):
        """Test stop handles consumer not running."""
        eventhub_consumer.running = False
        await eventhub_consumer.stop()
        # Test passes if no exception

    @pytest.mark.asyncio
    async def test_stop_with_no_tasks(self, eventhub_consumer):
        """Test stop with empty task set."""
        eventhub_consumer.running = True
        eventhub_consumer.tasks.clear()
        
        await eventhub_consumer.stop()
        
        assert eventhub_consumer.running is False
