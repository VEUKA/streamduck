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


# ============================================================================
# Additional Error Handling and Edge Case Tests
# ============================================================================


class TestEventHubMessageWrapper:
    """Test EventHubMessage wrapper class."""

    def test_message_to_dict_basic(self):
        """Test converting EventHub message to dictionary."""
        from src.consumers.eventhub import EventHubMessage
        from datetime import datetime, timezone
        
        mock_event = MagicMock(spec=EventData)
        mock_event.body_as_str.return_value = '{"test": "data"}'
        mock_event.enqueued_time = datetime.now(timezone.utc)
        mock_event.properties = {"key": "value"}
        mock_event.system_properties = {"sys_key": "sys_value"}
        
        message = EventHubMessage(
            event_data=mock_event,
            partition_id="0",
            sequence_number=123
        )
        
        result = message.to_dict()
        
        assert result["partition_id"] == "0"
        assert result["sequence_number"] == 123
        assert result["event_body"] == '{"test": "data"}'
        assert result["properties"] is not None
        assert result["system_properties"] is not None
        assert "ingestion_timestamp" in result

    def test_message_to_dict_with_bytes_properties(self):
        """Test message conversion handles bytes in properties."""
        from src.consumers.eventhub import EventHubMessage
        from datetime import datetime, timezone
        
        mock_event = MagicMock(spec=EventData)
        mock_event.body_as_str.return_value = "test"
        mock_event.enqueued_time = datetime.now(timezone.utc)
        mock_event.properties = {"key": b"bytes_value"}
        mock_event.system_properties = {"sys_key": b"sys_bytes"}
        
        message = EventHubMessage(
            event_data=mock_event,
            partition_id="0",
            sequence_number=123
        )
        
        result = message.to_dict()
        
        # Should not have bytes in result
        assert not any(isinstance(v, bytes) for v in result.values())

    def test_message_to_dict_no_properties(self):
        """Test message conversion with no properties."""
        from src.consumers.eventhub import EventHubMessage
        from datetime import datetime, timezone
        
        mock_event = MagicMock(spec=EventData)
        mock_event.body_as_str.return_value = "test"
        mock_event.enqueued_time = datetime.now(timezone.utc)
        mock_event.properties = None
        mock_event.system_properties = None
        
        message = EventHubMessage(
            event_data=mock_event,
            partition_id="0",
            sequence_number=123
        )
        
        result = message.to_dict()
        
        assert result["properties"] is None
        assert result["system_properties"] is None

    def test_message_to_dict_no_enqueued_time(self):
        """Test message conversion with no enqueued time."""
        from src.consumers.eventhub import EventHubMessage
        
        mock_event = MagicMock(spec=EventData)
        mock_event.body_as_str.return_value = "test"
        mock_event.enqueued_time = None
        mock_event.properties = None
        mock_event.system_properties = None
        
        message = EventHubMessage(
            event_data=mock_event,
            partition_id="0",
            sequence_number=123
        )
        
        result = message.to_dict()
        
        assert result["enqueued_time"] is None


class TestBytesConversion:
    """Test bytes to string conversion utilities."""

    def test_convert_bytes_to_str_simple_bytes(self):
        """Test converting simple bytes to string."""
        from src.consumers.eventhub import _convert_bytes_to_str
        
        result = _convert_bytes_to_str(b"hello")
        
        assert result == "hello"
        assert isinstance(result, str)

    def test_convert_bytes_to_str_dict_with_bytes(self):
        """Test converting dict with bytes values."""
        from src.consumers.eventhub import _convert_bytes_to_str
        
        input_dict = {
            b"key1": b"value1",
            "key2": b"value2",
            b"key3": "value3"
        }
        
        result = _convert_bytes_to_str(input_dict)
        
        assert all(isinstance(k, str) for k in result.keys())
        assert all(isinstance(v, str) for v in result.values())

    def test_convert_bytes_to_str_list_with_bytes(self):
        """Test converting list with bytes."""
        from src.consumers.eventhub import _convert_bytes_to_str
        
        input_list = [b"item1", "item2", b"item3"]
        
        result = _convert_bytes_to_str(input_list)
        
        assert isinstance(result, list)
        assert all(isinstance(item, str) for item in result)

    def test_convert_bytes_to_str_tuple_with_bytes(self):
        """Test converting tuple with bytes."""
        from src.consumers.eventhub import _convert_bytes_to_str
        
        input_tuple = (b"item1", "item2", b"item3")
        
        result = _convert_bytes_to_str(input_tuple)
        
        assert isinstance(result, tuple)
        assert all(isinstance(item, str) for item in result)

    def test_convert_bytes_to_str_nested_structure(self):
        """Test converting nested structure with bytes."""
        from src.consumers.eventhub import _convert_bytes_to_str
        
        input_data = {
            "top": {
                b"nested": [b"item1", {"deep": b"value"}]
            }
        }
        
        result = _convert_bytes_to_str(input_data)
        
        # Check no bytes remain
        import json
        # If this doesn't raise, no bytes remain
        json.dumps(result)

    def test_convert_bytes_to_str_preserves_non_bytes(self):
        """Test that non-bytes types are preserved."""
        from src.consumers.eventhub import _convert_bytes_to_str
        
        assert _convert_bytes_to_str(123) == 123
        assert _convert_bytes_to_str(45.67) == 45.67
        assert _convert_bytes_to_str(None) is None
        assert _convert_bytes_to_str(True) is True


class TestBytesEncoder:
    """Test BytesEncoder JSON encoder."""

    def test_bytes_encoder_converts_bytes(self):
        """Test BytesEncoder handles bytes objects."""
        from src.consumers.eventhub import BytesEncoder
        import json
        
        data = {"key": "value", "bytes_val": b"test"}
        
        # This should not raise
        result = json.dumps(data, cls=BytesEncoder)
        
        assert isinstance(result, str)
        assert "test" in result

    def test_bytes_encoder_handles_invalid_utf8(self):
        """Test BytesEncoder handles invalid UTF-8."""
        from src.consumers.eventhub import BytesEncoder
        import json
        
        # Invalid UTF-8 bytes
        data = {"bytes": b"\xff\xfe"}
        
        # Should not raise, uses 'replace' error handling
        result = json.dumps(data, cls=BytesEncoder)
        
        assert isinstance(result, str)


class TestMotherDuckCheckpointStore:
    """Test MotherDuckCheckpointStore (Azure SDK compatibility layer)."""

    @pytest.mark.asyncio
    async def test_list_ownership_empty(self):
        """Test listing ownership when no partitions claimed."""
        from src.consumers.eventhub import MotherDuckCheckpointStore, MotherDuckCheckpointManager
        
        mock_manager = MagicMock(spec=MotherDuckCheckpointManager)
        store = MotherDuckCheckpointStore(mock_manager)
        
        result = await store.list_ownership(
            fully_qualified_namespace="test.servicebus.windows.net",
            eventhub_name="test-hub",
            consumer_group="$Default"
        )
        
        assert isinstance(result, list)
        assert len(result) == 0

    @pytest.mark.asyncio
    async def test_claim_ownership_basic(self):
        """Test claiming ownership of partitions."""
        from src.consumers.eventhub import MotherDuckCheckpointStore, MotherDuckCheckpointManager
        
        mock_manager = MagicMock(spec=MotherDuckCheckpointManager)
        store = MotherDuckCheckpointStore(mock_manager)
        
        ownership_list = [
            {
                "fully_qualified_namespace": "test.servicebus.windows.net",
                "eventhub_name": "test-hub",
                "consumer_group": "$Default",
                "partition_id": "0",
                "owner_id": "consumer-1"
            }
        ]
        
        result = await store.claim_ownership(ownership_list)
        
        assert len(result) == 1
        assert result[0]["partition_id"] == "0"
        assert "etag" in result[0]

    @pytest.mark.asyncio
    async def test_update_checkpoint_with_valid_offset(self):
        """Test updating checkpoint with valid offset."""
        from src.consumers.eventhub import MotherDuckCheckpointStore, MotherDuckCheckpointManager
        
        mock_manager = AsyncMock(spec=MotherDuckCheckpointManager)
        mock_manager.save_checkpoint = AsyncMock(return_value=True)
        store = MotherDuckCheckpointStore(mock_manager)
        
        checkpoint = {
            "fully_qualified_namespace": "test.servicebus.windows.net",
            "eventhub_name": "test-hub",
            "consumer_group": "$Default",
            "partition_id": "0",
            "offset": "12345",
            "sequence_number": 100
        }
        
        await store.update_checkpoint(checkpoint)
        
        mock_manager.save_checkpoint.assert_called_once()

    @pytest.mark.asyncio
    async def test_update_checkpoint_with_invalid_offset(self):
        """Test updating checkpoint with invalid offset falls back to sequence."""
        from src.consumers.eventhub import MotherDuckCheckpointStore, MotherDuckCheckpointManager
        
        mock_manager = AsyncMock(spec=MotherDuckCheckpointManager)
        mock_manager.save_checkpoint = AsyncMock(return_value=True)
        store = MotherDuckCheckpointStore(mock_manager)
        
        checkpoint = {
            "fully_qualified_namespace": "test.servicebus.windows.net",
            "eventhub_name": "test-hub",
            "consumer_group": "$Default",
            "partition_id": "0",
            "offset": "invalid",
            "sequence_number": 100
        }
        
        # Should not raise, should use sequence_number as fallback
        await store.update_checkpoint(checkpoint)
        
        mock_manager.save_checkpoint.assert_called_once()

    @pytest.mark.asyncio
    async def test_update_checkpoint_save_failure(self):
        """Test handling of checkpoint save failure."""
        from src.consumers.eventhub import MotherDuckCheckpointStore, MotherDuckCheckpointManager
        
        mock_manager = AsyncMock(spec=MotherDuckCheckpointManager)
        mock_manager.save_checkpoint = AsyncMock(return_value=False)
        store = MotherDuckCheckpointStore(mock_manager)
        
        checkpoint = {
            "fully_qualified_namespace": "test.servicebus.windows.net",
            "eventhub_name": "test-hub",
            "consumer_group": "$Default",
            "partition_id": "0",
            "offset": "12345",
            "sequence_number": 100
        }
        
        # Should not raise even on failure
        await store.update_checkpoint(checkpoint)
        
        mock_manager.save_checkpoint.assert_called_once()


class TestEventHubConsumerAdvanced:
    """Advanced tests for EventHub consumer."""

    @pytest.mark.asyncio
    async def test_start_already_running(self, eventhub_consumer):
        """Test starting consumer that's already running."""
        eventhub_consumer.running = True
        
        # Should return early without error
        await eventhub_consumer.start()
        
        # Should still be running
        assert eventhub_consumer.running is True
