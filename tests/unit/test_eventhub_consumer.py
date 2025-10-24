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


# ============================================================================
# Credential Initialization Tests
# ============================================================================


class TestCredentialInitialization:
    """Test Azure credential initialization flow during start()."""

    @pytest.mark.asyncio
    async def test_start_with_successful_credential_initialization(self, eventhub_consumer):
        """Test consumer start with successful credential initialization."""
        from datetime import datetime, timedelta
        
        # Mock DefaultAzureCredential
        mock_credential = AsyncMock()
        mock_token = MagicMock()
        mock_token.token = "test_token_12345"
        mock_token.expires_on = (datetime.now() + timedelta(hours=1)).timestamp()
        mock_credential.get_token = AsyncMock(return_value=mock_token)
        
        # Mock EventHubConsumerClient
        mock_client = AsyncMock()
        mock_client.receive_batch = AsyncMock()
        
        # Mock checkpoint store creation
        mock_checkpoint_store = MagicMock()
        
        with patch("azure.identity.aio.DefaultAzureCredential", return_value=mock_credential):
            with patch("src.consumers.eventhub.EventHubConsumerClient", return_value=mock_client):
                with patch("src.consumers.eventhub.MotherDuckCheckpointStore", return_value=mock_checkpoint_store):
                    # Start will initialize credentials
                    await eventhub_consumer.start()
                    
                    # Should have called get_token to test credential
                    mock_credential.get_token.assert_called_once_with("https://eventhubs.azure.net/.default")
                    # Consumer should be running
                    assert eventhub_consumer.running is True

    @pytest.mark.asyncio
    async def test_start_logs_token_expiry(self, eventhub_consumer):
        """Test that token expiry time is logged during start()."""
        from datetime import datetime, timedelta
        
        mock_credential = AsyncMock()
        mock_token = MagicMock()
        mock_token.token = "test_token_12345"
        expiry_time = (datetime.now() + timedelta(hours=2)).timestamp()
        mock_token.expires_on = expiry_time
        mock_credential.get_token = AsyncMock(return_value=mock_token)
        
        mock_client = AsyncMock()
        mock_checkpoint_store = MagicMock()
        
        with patch("azure.identity.aio.DefaultAzureCredential", return_value=mock_credential):
            with patch("src.consumers.eventhub.EventHubConsumerClient", return_value=mock_client):
                with patch("src.consumers.eventhub.MotherDuckCheckpointStore", return_value=mock_checkpoint_store):
                    with patch("src.consumers.eventhub.logger") as mock_logger:
                        await eventhub_consumer.start()
                        
                        # Should log token expiry
                        info_calls = [str(call) for call in mock_logger.info.call_args_list]
                        assert any("Token expires" in call for call in info_calls)

    @pytest.mark.asyncio
    async def test_start_fails_on_auth_error(self, eventhub_consumer):
        """Test consumer start failure with authentication error."""
        from azure.core.exceptions import ClientAuthenticationError
        
        mock_credential = AsyncMock()
        mock_credential.get_token = AsyncMock(
            side_effect=ClientAuthenticationError("Authentication failed")
        )
        
        with patch("azure.identity.aio.DefaultAzureCredential", return_value=mock_credential):
            with pytest.raises(ClientAuthenticationError):
                await eventhub_consumer.start()
            
            # Consumer should not be running
            assert eventhub_consumer.running is False

    @pytest.mark.asyncio
    async def test_start_fails_on_credential_unavailable(self, eventhub_consumer):
        """Test consumer start when no credentials are available."""
        from azure.identity import CredentialUnavailableError
        
        # Mock credential creation to fail
        with patch("azure.identity.aio.DefaultAzureCredential", side_effect=CredentialUnavailableError("No credentials available")):
            with pytest.raises(CredentialUnavailableError):
                await eventhub_consumer.start()
            
            # Consumer should not be running
            assert eventhub_consumer.running is False

    @pytest.mark.asyncio
    async def test_start_logs_troubleshooting_on_credential_failure(self, eventhub_consumer):
        """Test that troubleshooting steps are logged on credential failure."""
        from azure.core.exceptions import ClientAuthenticationError
        
        mock_credential = AsyncMock()
        mock_credential.get_token = AsyncMock(
            side_effect=ClientAuthenticationError("Auth failed")
        )
        
        with patch("azure.identity.aio.DefaultAzureCredential", return_value=mock_credential):
            with patch("src.consumers.eventhub.logger") as mock_logger:
                try:
                    await eventhub_consumer.start()
                except ClientAuthenticationError:
                    pass
                
                # Should log troubleshooting steps
                error_calls = [str(call) for call in mock_logger.error.call_args_list]
                assert any("TROUBLESHOOTING" in call for call in error_calls)
                assert any("az login" in call for call in error_calls)


# ============================================================================
# Consumer Start Lifecycle Tests
# ============================================================================


class TestConsumerStartLifecycle:
    """Test consumer start() method lifecycle."""

    @pytest.mark.asyncio
    async def test_start_creates_checkpoint_store(self, eventhub_consumer):
        """Test that start() creates MotherDuckCheckpointStore."""
        mock_credential = AsyncMock()
        mock_token = MagicMock()
        mock_token.expires_on = 1234567890
        mock_credential.get_token = AsyncMock(return_value=mock_token)
        
        mock_client = AsyncMock()
        mock_checkpoint_store = MagicMock()
        
        with patch("azure.identity.aio.DefaultAzureCredential", return_value=mock_credential):
            with patch("src.consumers.eventhub.EventHubConsumerClient", return_value=mock_client):
                with patch("src.consumers.eventhub.MotherDuckCheckpointStore", return_value=mock_checkpoint_store) as mock_store_class:
                    await eventhub_consumer.start()
                    
                    # Should have created checkpoint store
                    mock_store_class.assert_called_once()
                    assert eventhub_consumer.checkpoint_manager is not None

    @pytest.mark.asyncio
    async def test_start_creates_eventhub_client_with_correct_params(self, eventhub_consumer):
        """Test that EventHubConsumerClient is created with correct parameters."""
        mock_credential = AsyncMock()
        mock_token = MagicMock()
        mock_token.expires_on = 1234567890
        mock_credential.get_token = AsyncMock(return_value=mock_token)
        
        mock_client = AsyncMock()
        mock_checkpoint_store = MagicMock()
        
        with patch("azure.identity.aio.DefaultAzureCredential", return_value=mock_credential):
            with patch("src.consumers.eventhub.EventHubConsumerClient", return_value=mock_client) as mock_client_class:
                with patch("src.consumers.eventhub.MotherDuckCheckpointStore", return_value=mock_checkpoint_store):
                    await eventhub_consumer.start()
                    
                    # Verify client was created with correct parameters
                    mock_client_class.assert_called_once()
                    call_kwargs = mock_client_class.call_args.kwargs
                    assert call_kwargs["fully_qualified_namespace"] == eventhub_consumer.eventhub_config.namespace
                    assert call_kwargs["eventhub_name"] == eventhub_consumer.eventhub_config.name
                    assert call_kwargs["consumer_group"] == eventhub_consumer.eventhub_config.consumer_group
                    assert call_kwargs["checkpoint_store"] is mock_checkpoint_store

    @pytest.mark.asyncio
    async def test_start_initializes_message_batch(self, eventhub_consumer):
        """Test that start() initializes MessageBatch."""
        mock_credential = AsyncMock()
        mock_token = MagicMock()
        mock_token.expires_on = 1234567890
        mock_credential.get_token = AsyncMock(return_value=mock_token)
        
        mock_client = AsyncMock()
        mock_checkpoint_store = MagicMock()
        
        with patch("azure.identity.aio.DefaultAzureCredential", return_value=mock_credential):
            with patch("src.consumers.eventhub.EventHubConsumerClient", return_value=mock_client):
                with patch("src.consumers.eventhub.MotherDuckCheckpointStore", return_value=mock_checkpoint_store):
                    await eventhub_consumer.start()
                    
                    # Should have initialized batch
                    assert eventhub_consumer.current_batch is not None
                    assert eventhub_consumer.current_batch.max_size == eventhub_consumer.batch_size

    @pytest.mark.asyncio
    async def test_start_sets_running_flag_and_start_time(self, eventhub_consumer):
        """Test that start() sets running flag and start time."""
        from datetime import datetime, UTC
        
        mock_credential = AsyncMock()
        mock_token = MagicMock()
        mock_token.expires_on = 1234567890
        mock_credential.get_token = AsyncMock(return_value=mock_token)
        
        mock_client = AsyncMock()
        mock_checkpoint_store = MagicMock()
        
        with patch("azure.identity.aio.DefaultAzureCredential", return_value=mock_credential):
            with patch("src.consumers.eventhub.EventHubConsumerClient", return_value=mock_client):
                with patch("src.consumers.eventhub.MotherDuckCheckpointStore", return_value=mock_checkpoint_store):
                    before_start = datetime.now(UTC)
                    await eventhub_consumer.start()
                    after_start = datetime.now(UTC)
                    
                    # Should be running
                    assert eventhub_consumer.running is True
                    # Start time should be set
                    assert eventhub_consumer.stats["start_time"] is not None
                    # Start time should be between before and after
                    start_time = eventhub_consumer.stats["start_time"]
                    assert before_start <= start_time <= after_start

    @pytest.mark.asyncio
    async def test_start_creates_batch_timeout_task(self, eventhub_consumer):
        """Test that start() creates batch timeout handler task."""
        mock_credential = AsyncMock()
        mock_token = MagicMock()
        mock_token.expires_on = 1234567890
        mock_credential.get_token = AsyncMock(return_value=mock_token)
        
        mock_client = AsyncMock()
        mock_checkpoint_store = MagicMock()
        
        with patch("azure.identity.aio.DefaultAzureCredential", return_value=mock_credential):
            with patch("src.consumers.eventhub.EventHubConsumerClient", return_value=mock_client):
                with patch("src.consumers.eventhub.MotherDuckCheckpointStore", return_value=mock_checkpoint_store):
                    await eventhub_consumer.start()
                    
                    # Should have created timeout task
                    assert len(eventhub_consumer.tasks) > 0

    @pytest.mark.asyncio
    async def test_start_calls_client_receive(self, eventhub_consumer):
        """Test that start() calls client.receive() with on_event callback."""
        mock_credential = AsyncMock()
        mock_token = MagicMock()
        mock_token.expires_on = 1234567890
        mock_credential.get_token = AsyncMock(return_value=mock_token)
        
        mock_client = AsyncMock()
        # Make receive() raise exception to exit early
        mock_client.receive = AsyncMock(side_effect=KeyboardInterrupt())
        mock_checkpoint_store = MagicMock()
        
        with patch("azure.identity.aio.DefaultAzureCredential", return_value=mock_credential):
            with patch("src.consumers.eventhub.EventHubConsumerClient", return_value=mock_client):
                with patch("src.consumers.eventhub.MotherDuckCheckpointStore", return_value=mock_checkpoint_store):
                    try:
                        await eventhub_consumer.start()
                    except KeyboardInterrupt:
                        pass
                    
                    # Should have called receive with on_event callback
                    mock_client.receive.assert_called_once()
                    call_kwargs = mock_client.receive.call_args.kwargs
                    assert "on_event" in call_kwargs
                    assert call_kwargs["on_event"] == eventhub_consumer._on_event

    @pytest.mark.asyncio
    async def test_start_handles_rbac_error(self, eventhub_consumer):
        """Test that start() handles RBAC permission errors appropriately."""
        from azure.core.exceptions import ServiceRequestError
        
        mock_credential = AsyncMock()
        mock_token = MagicMock()
        mock_token.expires_on = 1234567890
        mock_credential.get_token = AsyncMock(return_value=mock_token)
        
        mock_client = AsyncMock()
        mock_client.receive = AsyncMock(
            side_effect=ServiceRequestError("Unauthorized access")
        )
        mock_checkpoint_store = MagicMock()
        
        with patch("azure.identity.aio.DefaultAzureCredential", return_value=mock_credential):
            with patch("src.consumers.eventhub.EventHubConsumerClient", return_value=mock_client):
                with patch("src.consumers.eventhub.MotherDuckCheckpointStore", return_value=mock_checkpoint_store):
                    with patch("src.consumers.eventhub.logger") as mock_logger:
                        with pytest.raises(RuntimeError, match="Missing Azure RBAC permission"):
                            await eventhub_consumer.start()
                        
                        # Should log RBAC-related error
                        error_calls = [str(call) for call in mock_logger.error.call_args_list]
                        assert any("ERROR" in call for call in error_calls)

    @pytest.mark.asyncio
    async def test_start_early_return_if_already_running(self, eventhub_consumer):
        """Test that start() returns early if consumer is already running."""
        eventhub_consumer.running = True
        
        # Should not raise, just return early
        await eventhub_consumer.start()
        
        # Client should not be created
        assert eventhub_consumer.client is None


# ============================================================================
# Consumer Stop and Cleanup Tests
# ============================================================================


class TestConsumerStopAndCleanup:
    """Test consumer stop() method and cleanup."""

    @pytest.mark.asyncio
    async def test_stop_when_not_running(self, eventhub_consumer):
        """Test stop() returns early when consumer is not running."""
        eventhub_consumer.running = False
        
        # Should not raise, just return early
        await eventhub_consumer.stop()
        
        # Should remain not running
        assert eventhub_consumer.running is False

    @pytest.mark.asyncio
    async def test_stop_sets_running_to_false(self, eventhub_consumer):
        """Test that stop() sets running flag to False."""
        eventhub_consumer.running = True
        
        await eventhub_consumer.stop()
        
        assert eventhub_consumer.running is False

    @pytest.mark.asyncio
    async def test_stop_closes_eventhub_client(self, eventhub_consumer):
        """Test that stop() closes the EventHub client."""
        eventhub_consumer.running = True
        mock_client = AsyncMock()
        mock_client.close = AsyncMock()
        eventhub_consumer.client = mock_client
        
        await eventhub_consumer.stop()
        
        # Client should be closed
        mock_client.close.assert_called_once()
        # Client reference should be None
        assert eventhub_consumer.client is None

    @pytest.mark.asyncio
    async def test_stop_handles_client_close_error(self, eventhub_consumer):
        """Test that stop() handles errors during client close."""
        eventhub_consumer.running = True
        mock_client = AsyncMock()
        mock_client.close = AsyncMock(side_effect=Exception("Close error"))
        eventhub_consumer.client = mock_client
        
        # Should not raise, just log warning
        with patch("src.consumers.eventhub.logger") as mock_logger:
            await eventhub_consumer.stop()
            
            # Should log warning
            warning_calls = [str(call) for call in mock_logger.warning.call_args_list]
            assert any("Error closing" in call for call in warning_calls)
        
        # Client should still be set to None
        assert eventhub_consumer.client is None

    @pytest.mark.asyncio
    async def test_stop_cancels_tasks(self, eventhub_consumer):
        """Test that stop() cancels all running tasks."""
        eventhub_consumer.running = True
        
        import asyncio
        
        # Create real async tasks
        async def dummy_task():
            await asyncio.sleep(10)  # Long-running task
        
        async def finished_task():
            pass  # Completes immediately
        
        task1 = asyncio.create_task(dummy_task())
        task2 = asyncio.create_task(finished_task())
        
        # Wait for task2 to finish
        await asyncio.sleep(0.01)
        
        eventhub_consumer.tasks = {task1, task2}
        
        # Track if task1 gets cancelled
        assert not task1.done()
        assert task2.done()
        
        await eventhub_consumer.stop()
        
        # Task1 should be cancelled
        assert task1.cancelled()
        # Tasks set should be cleared
        assert len(eventhub_consumer.tasks) == 0

    @pytest.mark.asyncio
    async def test_stop_processes_remaining_batch(self, eventhub_consumer):
        """Test that stop() processes remaining messages in batch."""
        eventhub_consumer.running = True
        
        # Create a batch with messages
        from src.consumers.eventhub import MessageBatch, EventHubMessage
        from azure.eventhub import EventData
        mock_batch = MessageBatch(max_size=1000, max_wait_seconds=60)
        
        # Add mock messages
        mock_event = MagicMock(spec=EventData)
        mock_event.sequence_number = 123
        mock_event.offset = "456"
        mock_event.enqueued_time = None
        mock_event.body = b"test"
        mock_event.properties = {}
        
        mock_message = EventHubMessage(
            event_data=mock_event,
            partition_id="0",
            sequence_number=123
        )
        mock_batch.add_message(mock_message)
        
        eventhub_consumer.current_batch = mock_batch
        
        # Mock _process_batch
        eventhub_consumer._process_batch = AsyncMock()
        
        await eventhub_consumer.stop()
        
        # Should have processed the batch
        eventhub_consumer._process_batch.assert_called_once_with(mock_batch)

    @pytest.mark.asyncio
    async def test_stop_skips_processing_empty_batch(self, eventhub_consumer):
        """Test that stop() skips processing if batch is empty."""
        eventhub_consumer.running = True
        
        from src.consumers.eventhub import MessageBatch
        mock_batch = MessageBatch(max_size=1000, max_wait_seconds=60)
        eventhub_consumer.current_batch = mock_batch
        
        # Mock _process_batch
        eventhub_consumer._process_batch = AsyncMock()
        
        with patch("src.consumers.eventhub.logger") as mock_logger:
            await eventhub_consumer.stop()
        
        # Should not process empty batch
        eventhub_consumer._process_batch.assert_not_called()
        # Should log "No remaining messages"
        info_calls = [str(call) for call in mock_logger.info.call_args_list]
        assert any("No remaining messages" in call for call in info_calls)

    @pytest.mark.asyncio
    async def test_stop_handles_batch_processing_error(self, eventhub_consumer):
        """Test that stop() handles errors during batch processing."""
        eventhub_consumer.running = True
        
        from src.consumers.eventhub import MessageBatch, EventHubMessage
        from azure.eventhub import EventData
        mock_batch = MessageBatch(max_size=1000, max_wait_seconds=60)
        
        # Add a message to batch
        mock_event = MagicMock(spec=EventData)
        mock_event.sequence_number = 123
        mock_event.offset = "456"
        mock_event.enqueued_time = None
        mock_event.body = b"test"
        mock_event.properties = {}
        
        mock_message = EventHubMessage(
            event_data=mock_event,
            partition_id="0",
            sequence_number=123
        )
        mock_batch.add_message(mock_message)
        eventhub_consumer.current_batch = mock_batch
        
        # Mock _process_batch to raise error
        eventhub_consumer._process_batch = AsyncMock(side_effect=Exception("Processing error"))
        
        with patch("src.consumers.eventhub.logger") as mock_logger:
            # Should not raise, just log error
            await eventhub_consumer.stop()
            
            # Should log error
            error_calls = [str(call) for call in mock_logger.error.call_args_list]
            assert any("Error processing remaining batch" in call for call in error_calls)

    @pytest.mark.asyncio
    async def test_stop_closes_checkpoint_manager(self, eventhub_consumer):
        """Test that stop() closes the checkpoint manager."""
        eventhub_consumer.running = True
        
        mock_checkpoint_manager = MagicMock()
        mock_checkpoint_manager.close = MagicMock()
        eventhub_consumer.checkpoint_manager = mock_checkpoint_manager
        
        await eventhub_consumer.stop()
        
        # Should close checkpoint manager
        mock_checkpoint_manager.close.assert_called_once()
        # Reference should be None
        assert eventhub_consumer.checkpoint_manager is None

    @pytest.mark.asyncio
    async def test_stop_logs_graceful_shutdown_complete(self, eventhub_consumer):
        """Test that stop() logs completion of graceful shutdown."""
        eventhub_consumer.running = True
        
        with patch("src.consumers.eventhub.logger") as mock_logger:
            await eventhub_consumer.stop()
            
            # Should log graceful shutdown messages
            info_calls = [str(call) for call in mock_logger.info.call_args_list]
            assert any("Stopping EventHub consumer gracefully" in call for call in info_calls)
            assert any("stopped gracefully" in call for call in info_calls)


class TestOnEventCallback:
    """Test cases for _on_event message callback."""

    @pytest.mark.asyncio
    async def test_on_event_ignores_when_not_running(self, eventhub_consumer):
        """Test that _on_event ignores events when consumer is not running."""
        eventhub_consumer.running = False
        
        from azure.eventhub import EventData
        mock_partition_context = MagicMock()
        mock_partition_context.partition_id = "0"
        
        mock_event = MagicMock(spec=EventData)
        mock_event.sequence_number = 123
        mock_event.offset = "456"
        
        initial_stats = eventhub_consumer.stats["messages_received"]
        
        with patch("src.consumers.eventhub.logger") as mock_logger:
            await eventhub_consumer._on_event(mock_partition_context, mock_event)
            
            # Should log debug message
            debug_calls = [str(call) for call in mock_logger.debug.call_args_list]
            assert any("Consumer not running" in call for call in debug_calls)
        
        # Should not increment message count
        assert eventhub_consumer.stats["messages_received"] == initial_stats

    @pytest.mark.asyncio
    async def test_on_event_ignores_none_event(self, eventhub_consumer):
        """Test that _on_event handles None events gracefully."""
        eventhub_consumer.running = True
        
        mock_partition_context = MagicMock()
        mock_partition_context.partition_id = "0"
        
        initial_stats = eventhub_consumer.stats["messages_received"]
        
        with patch("src.consumers.eventhub.logger") as mock_logger:
            await eventhub_consumer._on_event(mock_partition_context, None)
            
            # Should log debug message
            debug_calls = [str(call) for call in mock_logger.debug.call_args_list]
            assert any("Received None event" in call for call in debug_calls)
        
        # Should not increment message count
        assert eventhub_consumer.stats["messages_received"] == initial_stats

    @pytest.mark.asyncio
    async def test_on_event_logs_first_message_per_partition(self, eventhub_consumer):
        """Test that _on_event logs the first message received on each partition."""
        eventhub_consumer.running = True
        
        from src.consumers.eventhub import MessageBatch
        from azure.eventhub import EventData
        eventhub_consumer.current_batch = MessageBatch(max_size=1000, max_wait_seconds=60)
        
        mock_partition_context = MagicMock()
        mock_partition_context.partition_id = "0"
        
        mock_event = MagicMock(spec=EventData)
        mock_event.sequence_number = 123
        mock_event.offset = "456"
        mock_event.enqueued_time = None
        mock_event.body = b"test"
        mock_event.properties = {}
        
        with patch("src.consumers.eventhub.logger") as mock_logger, \
             patch("src.consumers.eventhub.logfire") as mock_logfire:
            
            await eventhub_consumer._on_event(mock_partition_context, mock_event)
            
            # Should log warning for first message
            warning_calls = [str(call) for call in mock_logger.warning.call_args_list]
            assert any("FIRST MESSAGE on partition 0" in call for call in warning_calls)
            
            # Should log to Logfire
            mock_logfire.info.assert_called_once()
            call_args = mock_logfire.info.call_args
            assert call_args[0][0] == "First message on partition"
            assert call_args[1]["partition_id"] == "0"
            assert call_args[1]["offset"] == "456"
            assert call_args[1]["sequence_number"] == 123
        
        # Partition should be marked as logged
        assert "0" in eventhub_consumer._first_message_logged

    @pytest.mark.asyncio
    async def test_on_event_does_not_log_subsequent_messages(self, eventhub_consumer):
        """Test that _on_event doesn't log subsequent messages on same partition."""
        eventhub_consumer.running = True
        
        from src.consumers.eventhub import MessageBatch
        from azure.eventhub import EventData
        eventhub_consumer.current_batch = MessageBatch(max_size=1000, max_wait_seconds=60)
        
        # Mark partition as already logged
        eventhub_consumer._first_message_logged.add("0")
        
        mock_partition_context = MagicMock()
        mock_partition_context.partition_id = "0"
        
        mock_event = MagicMock(spec=EventData)
        mock_event.sequence_number = 124
        mock_event.offset = "789"
        mock_event.enqueued_time = None
        mock_event.body = b"test"
        mock_event.properties = {}
        
        with patch("src.consumers.eventhub.logger") as mock_logger, \
             patch("src.consumers.eventhub.logfire") as mock_logfire:
            
            await eventhub_consumer._on_event(mock_partition_context, mock_event)
            
            # Should NOT log warning for subsequent message
            warning_calls = [str(call) for call in mock_logger.warning.call_args_list]
            assert not any("FIRST MESSAGE" in call for call in warning_calls)
            
            # Should NOT log to Logfire
            mock_logfire.info.assert_not_called()

    @pytest.mark.asyncio
    async def test_on_event_rejects_none_sequence_number(self, eventhub_consumer):
        """Test that _on_event rejects events with None sequence_number."""
        eventhub_consumer.running = True
        
        from src.consumers.eventhub import MessageBatch
        from azure.eventhub import EventData
        eventhub_consumer.current_batch = MessageBatch(max_size=1000, max_wait_seconds=60)
        
        mock_partition_context = MagicMock()
        mock_partition_context.partition_id = "0"
        
        mock_event = MagicMock(spec=EventData)
        mock_event.sequence_number = None  # Invalid
        mock_event.offset = "456"
        mock_event.enqueued_time = None
        
        initial_stats = eventhub_consumer.stats["messages_received"]
        
        with patch("src.consumers.eventhub.logger") as mock_logger:
            await eventhub_consumer._on_event(mock_partition_context, mock_event)
            
            # Should log warning
            warning_calls = [str(call) for call in mock_logger.warning.call_args_list]
            assert any("None sequence_number" in call for call in warning_calls)
        
        # Should not increment message count
        assert eventhub_consumer.stats["messages_received"] == initial_stats

    @pytest.mark.asyncio
    async def test_on_event_creates_message_wrapper(self, eventhub_consumer):
        """Test that _on_event creates EventHubMessage wrapper correctly."""
        eventhub_consumer.running = True
        
        from src.consumers.eventhub import MessageBatch
        from azure.eventhub import EventData
        eventhub_consumer.current_batch = MessageBatch(max_size=1000, max_wait_seconds=60)
        
        mock_partition_context = MagicMock()
        mock_partition_context.partition_id = "0"
        
        mock_event = MagicMock(spec=EventData)
        mock_event.sequence_number = 123
        mock_event.offset = "456"
        mock_event.enqueued_time = None
        mock_event.body = b"test message"
        mock_event.properties = {"key": "value"}
        
        await eventhub_consumer._on_event(mock_partition_context, mock_event)
        
        # Should increment message count
        assert eventhub_consumer.stats["messages_received"] == 1
        
        # Message should be in batch
        assert len(eventhub_consumer.current_batch.messages) == 1
        message = eventhub_consumer.current_batch.messages[0]
        assert message.partition_id == "0"
        assert message.sequence_number == 123
        assert message.event_data == mock_event
        assert message.partition_context == mock_partition_context

    @pytest.mark.asyncio
    async def test_on_event_triggers_batch_processing_when_ready(self, eventhub_consumer):
        """Test that _on_event processes batch when it becomes ready."""
        eventhub_consumer.running = True
        
        from src.consumers.eventhub import MessageBatch
        from azure.eventhub import EventData
        
        # Create a batch with max_size=2 for testing
        eventhub_consumer.current_batch = MessageBatch(max_size=2, max_wait_seconds=60)
        
        mock_partition_context = MagicMock()
        mock_partition_context.partition_id = "0"
        
        # Mock _process_batch
        eventhub_consumer._process_batch = AsyncMock()
        
        # Add first message
        mock_event1 = MagicMock(spec=EventData)
        mock_event1.sequence_number = 123
        mock_event1.offset = "456"
        mock_event1.enqueued_time = None
        mock_event1.body = b"test1"
        mock_event1.properties = {}
        
        await eventhub_consumer._on_event(mock_partition_context, mock_event1)
        
        # Batch not ready yet
        eventhub_consumer._process_batch.assert_not_called()
        
        # Add second message - should trigger batch processing
        mock_event2 = MagicMock(spec=EventData)
        mock_event2.sequence_number = 124
        mock_event2.offset = "789"
        mock_event2.enqueued_time = None
        mock_event2.body = b"test2"
        mock_event2.properties = {}
        
        old_batch = eventhub_consumer.current_batch
        
        await eventhub_consumer._on_event(mock_partition_context, mock_event2)
        
        # Should have processed the batch
        eventhub_consumer._process_batch.assert_called_once_with(old_batch)
        
        # Should have created a new batch
        assert eventhub_consumer.current_batch != old_batch
        assert len(eventhub_consumer.current_batch.messages) == 0

    @pytest.mark.asyncio
    async def test_on_event_handles_processing_errors(self, eventhub_consumer):
        """Test that _on_event handles errors gracefully."""
        eventhub_consumer.running = True
        
        from src.consumers.eventhub import MessageBatch
        from azure.eventhub import EventData
        eventhub_consumer.current_batch = MessageBatch(max_size=1000, max_wait_seconds=60)
        
        mock_partition_context = MagicMock()
        mock_partition_context.partition_id = "0"
        
        # Create an event that will cause an error
        mock_event = MagicMock(spec=EventData)
        mock_event.sequence_number = 123
        mock_event.offset = "456"
        mock_event.enqueued_time = None
        mock_event.body = b"test"
        mock_event.properties = {}
        
        # Make add_message raise an error
        with patch.object(eventhub_consumer.current_batch, 'add_message', side_effect=Exception("Test error")):
            with patch("src.consumers.eventhub.logger") as mock_logger, \
                 patch("src.consumers.eventhub.logfire") as mock_logfire:
                
                # Should not raise, just log error
                await eventhub_consumer._on_event(mock_partition_context, mock_event)
                
                # Should log error
                error_calls = [str(call) for call in mock_logger.error.call_args_list]
                assert any("Error processing event" in call for call in error_calls)
                
                # Should log to Logfire
                mock_logfire.error.assert_called_once()
                call_args = mock_logfire.error.call_args
                assert call_args[0][0] == "Error processing event"
                assert call_args[1]["partition_id"] == "0"


class TestProcessBatch:
    """Test cases for _process_batch implementation."""

    @pytest.mark.asyncio
    async def test_process_batch_handles_empty_batch(self, eventhub_consumer):
        """Test that _process_batch handles empty batches gracefully."""
        from src.consumers.eventhub import MessageBatch
        
        empty_batch = MessageBatch(max_size=1000, max_wait_seconds=60)
        
        with patch("src.consumers.eventhub.logfire") as mock_logfire, \
             patch("src.consumers.eventhub.logger") as mock_logger:
            
            # Create a mock span context manager
            mock_span = MagicMock()
            mock_logfire.span.return_value.__enter__ = MagicMock(return_value=mock_span)
            mock_logfire.span.return_value.__exit__ = MagicMock(return_value=False)
            
            await eventhub_consumer._process_batch(empty_batch)
            
            # Should set empty_batch attribute
            mock_span.set_attribute.assert_called_with("empty_batch", True)
            
            # Should log debug message
            debug_calls = [str(call) for call in mock_logger.debug.call_args_list]
            assert any("No messages in batch" in call for call in debug_calls)
        
        # Message processor should not be called
        eventhub_consumer.message_processor.assert_not_called()

    @pytest.mark.asyncio
    async def test_process_batch_calls_message_processor(self, eventhub_consumer):
        """Test that _process_batch calls the message processor with messages."""
        from src.consumers.eventhub import MessageBatch, EventHubMessage
        from azure.eventhub import EventData
        
        batch = MessageBatch(max_size=1000, max_wait_seconds=60)
        
        # Add messages to batch
        mock_partition_context = MagicMock()
        mock_partition_context.update_checkpoint = AsyncMock()
        
        for i in range(3):
            mock_event = MagicMock(spec=EventData)
            mock_event.sequence_number = 100 + i
            mock_event.offset = str(200 + i)
            mock_event.body = f"test{i}".encode()
            mock_event.properties = {}
            
            message = EventHubMessage(
                event_data=mock_event,
                partition_id="0",
                sequence_number=100 + i
            )
            message.partition_context = mock_partition_context
            batch.add_message(message)
        
        # Mock message processor to return success
        eventhub_consumer.message_processor = MagicMock(return_value=True)
        
        with patch("src.consumers.eventhub.logfire") as mock_logfire:
            mock_span = MagicMock()
            mock_logfire.span.return_value.__enter__ = MagicMock(return_value=mock_span)
            mock_logfire.span.return_value.__exit__ = MagicMock(return_value=False)
            
            await eventhub_consumer._process_batch(batch)
        
        # Should call message processor with all messages
        eventhub_consumer.message_processor.assert_called_once()
        call_args = eventhub_consumer.message_processor.call_args[0][0]
        assert len(call_args) == 3
        assert all(isinstance(msg, EventHubMessage) for msg in call_args)

    @pytest.mark.asyncio
    async def test_process_batch_updates_checkpoints_on_success(self, eventhub_consumer):
        """Test that _process_batch updates SDK checkpoints when processor succeeds."""
        from src.consumers.eventhub import MessageBatch, EventHubMessage
        from azure.eventhub import EventData
        
        batch = MessageBatch(max_size=1000, max_wait_seconds=60)
        
        # Add messages from two partitions
        mock_context_0 = MagicMock()
        mock_context_0.update_checkpoint = AsyncMock()
        
        mock_context_1 = MagicMock()
        mock_context_1.update_checkpoint = AsyncMock()
        
        # Partition 0 messages
        for i in range(2):
            mock_event = MagicMock(spec=EventData)
            mock_event.sequence_number = 100 + i
            mock_event.offset = str(200 + i)
            mock_event.body = b"test"
            
            message = EventHubMessage(
                event_data=mock_event,
                partition_id="0",
                sequence_number=100 + i
            )
            message.partition_context = mock_context_0
            batch.add_message(message)
        
        # Partition 1 message
        mock_event = MagicMock(spec=EventData)
        mock_event.sequence_number = 200
        mock_event.offset = "300"
        mock_event.body = b"test"
        
        message = EventHubMessage(
            event_data=mock_event,
            partition_id="1",
            sequence_number=200
        )
        message.partition_context = mock_context_1
        batch.add_message(message)
        
        # Mock message processor to return success
        eventhub_consumer.message_processor = MagicMock(return_value=True)
        
        with patch("src.consumers.eventhub.logfire") as mock_logfire:
            mock_span = MagicMock()
            mock_logfire.span.return_value.__enter__ = MagicMock(return_value=mock_span)
            mock_logfire.span.return_value.__exit__ = MagicMock(return_value=False)
            
            await eventhub_consumer._process_batch(batch)
        
        # Should update checkpoint for partition 0 (with last message)
        mock_context_0.update_checkpoint.assert_called_once()
        # Should update checkpoint for partition 1
        mock_context_1.update_checkpoint.assert_called_once()

    @pytest.mark.asyncio
    async def test_process_batch_handles_checkpoint_update_failure(self, eventhub_consumer):
        """Test that _process_batch handles checkpoint update failures gracefully."""
        from src.consumers.eventhub import MessageBatch, EventHubMessage
        from azure.eventhub import EventData
        
        batch = MessageBatch(max_size=1000, max_wait_seconds=60)
        
        # Add message with partition context that fails
        mock_partition_context = MagicMock()
        mock_partition_context.update_checkpoint = AsyncMock(
            side_effect=Exception("Checkpoint update failed")
        )
        
        mock_event = MagicMock(spec=EventData)
        mock_event.sequence_number = 100
        mock_event.offset = "200"
        mock_event.body = b"test"
        
        message = EventHubMessage(
            event_data=mock_event,
            partition_id="0",
            sequence_number=100
        )
        message.partition_context = mock_partition_context
        batch.add_message(message)
        
        # Mock message processor to return success
        eventhub_consumer.message_processor = MagicMock(return_value=True)
        
        with patch("src.consumers.eventhub.logfire") as mock_logfire, \
             patch("src.consumers.eventhub.logger") as mock_logger:
            
            mock_span = MagicMock()
            mock_logfire.span.return_value.__enter__ = MagicMock(return_value=mock_span)
            mock_logfire.span.return_value.__exit__ = MagicMock(return_value=False)
            
            # Should not raise, just log error
            await eventhub_consumer._process_batch(batch)
            
            # Should log error
            error_calls = [str(call) for call in mock_logger.error.call_args_list]
            assert any("Failed to update SDK checkpoint" in call for call in error_calls)
            
            # Should log to Logfire
            mock_logfire.error.assert_called_once()
            call_args = mock_logfire.error.call_args
            assert call_args[0][0] == "Checkpoint update failed"

    @pytest.mark.asyncio
    async def test_process_batch_warns_on_missing_partition_context(self, eventhub_consumer):
        """Test that _process_batch warns when partition_context is missing."""
        from src.consumers.eventhub import MessageBatch, EventHubMessage
        from azure.eventhub import EventData
        
        batch = MessageBatch(max_size=1000, max_wait_seconds=60)
        
        # Add message WITHOUT partition context
        mock_event = MagicMock(spec=EventData)
        mock_event.sequence_number = 100
        mock_event.offset = "200"
        mock_event.body = b"test"
        
        message = EventHubMessage(
            event_data=mock_event,
            partition_id="0",
            sequence_number=100
        )
        # Don't set partition_context
        batch.add_message(message)
        
        # Mock message processor to return success
        eventhub_consumer.message_processor = MagicMock(return_value=True)
        
        with patch("src.consumers.eventhub.logfire") as mock_logfire, \
             patch("src.consumers.eventhub.logger") as mock_logger:
            
            mock_span = MagicMock()
            mock_logfire.span.return_value.__enter__ = MagicMock(return_value=mock_span)
            mock_logfire.span.return_value.__exit__ = MagicMock(return_value=False)
            
            await eventhub_consumer._process_batch(batch)
            
            # Should log warning
            warning_calls = [str(call) for call in mock_logger.warning.call_args_list]
            assert any("No partition_context" in call for call in warning_calls)

    @pytest.mark.asyncio
    async def test_process_batch_updates_stats_on_success(self, eventhub_consumer):
        """Test that _process_batch updates stats when processing succeeds."""
        from src.consumers.eventhub import MessageBatch, EventHubMessage
        from azure.eventhub import EventData
        
        batch = MessageBatch(max_size=1000, max_wait_seconds=60)
        
        # Add messages
        mock_partition_context = MagicMock()
        mock_partition_context.update_checkpoint = AsyncMock()
        
        for i in range(5):
            mock_event = MagicMock(spec=EventData)
            mock_event.sequence_number = 100 + i
            mock_event.offset = str(200 + i)
            mock_event.body = b"test"
            
            message = EventHubMessage(
                event_data=mock_event,
                partition_id="0",
                sequence_number=100 + i
            )
            message.partition_context = mock_partition_context
            batch.add_message(message)
        
        # Mock message processor to return success
        eventhub_consumer.message_processor = MagicMock(return_value=True)
        
        initial_batches = eventhub_consumer.stats["batches_processed"]
        
        with patch("src.consumers.eventhub.logfire") as mock_logfire:
            mock_span = MagicMock()
            mock_logfire.span.return_value.__enter__ = MagicMock(return_value=mock_span)
            mock_logfire.span.return_value.__exit__ = MagicMock(return_value=False)
            
            await eventhub_consumer._process_batch(batch)
        
        # Should increment batches_processed
        assert eventhub_consumer.stats["batches_processed"] == initial_batches + 1
        
        # Should update last_checkpoint
        assert "last_checkpoint" in eventhub_consumer.stats
        assert eventhub_consumer.stats["last_checkpoint"]["0"] == 104  # Last sequence number

    @pytest.mark.asyncio
    async def test_process_batch_sets_span_attributes(self, eventhub_consumer):
        """Test that _process_batch sets Logfire span attributes correctly."""
        from src.consumers.eventhub import MessageBatch, EventHubMessage
        from azure.eventhub import EventData
        
        batch = MessageBatch(max_size=1000, max_wait_seconds=60)
        
        # Add messages from two partitions
        for partition_id in ["0", "1"]:
            mock_partition_context = MagicMock()
            mock_partition_context.update_checkpoint = AsyncMock()
            
            mock_event = MagicMock(spec=EventData)
            mock_event.sequence_number = 100
            mock_event.offset = "200"
            mock_event.body = b"test"
            
            message = EventHubMessage(
                event_data=mock_event,
                partition_id=partition_id,
                sequence_number=100
            )
            message.partition_context = mock_partition_context
            batch.add_message(message)
        
        # Mock message processor to return success
        eventhub_consumer.message_processor = MagicMock(return_value=True)
        
        with patch("src.consumers.eventhub.logfire") as mock_logfire:
            mock_span = MagicMock()
            mock_logfire.span.return_value.__enter__ = MagicMock(return_value=mock_span)
            mock_logfire.span.return_value.__exit__ = MagicMock(return_value=False)
            
            await eventhub_consumer._process_batch(batch)
            
            # Check span attributes were set
            set_attribute_calls = mock_span.set_attribute.call_args_list
            attributes = {call[0][0]: call[0][1] for call in set_attribute_calls}
            
            assert attributes["processor_success"] is True
            assert attributes["checkpoints_updated"] == 2
            assert attributes["partitions_count"] == 2
            assert "total_batches_processed" in attributes

    @pytest.mark.asyncio
    async def test_process_batch_handles_processor_failure(self, eventhub_consumer):
        """Test that _process_batch handles processor failure correctly."""
        from src.consumers.eventhub import MessageBatch, EventHubMessage
        from azure.eventhub import EventData
        
        batch = MessageBatch(max_size=1000, max_wait_seconds=60)
        
        # Add message
        mock_event = MagicMock(spec=EventData)
        mock_event.sequence_number = 100
        mock_event.offset = "200"
        mock_event.body = b"test"
        
        message = EventHubMessage(
            event_data=mock_event,
            partition_id="0",
            sequence_number=100
        )
        batch.add_message(message)
        
        # Mock message processor to return failure
        eventhub_consumer.message_processor = MagicMock(return_value=False)
        
        with patch("src.consumers.eventhub.logfire") as mock_logfire, \
             patch("src.consumers.eventhub.logger") as mock_logger:
            
            mock_span = MagicMock()
            mock_logfire.span.return_value.__enter__ = MagicMock(return_value=mock_span)
            mock_logfire.span.return_value.__exit__ = MagicMock(return_value=False)
            
            await eventhub_consumer._process_batch(batch)
            
            # Should log error
            error_calls = [str(call) for call in mock_logger.error.call_args_list]
            assert any("Message processor returned failure" in call for call in error_calls)
            
            # Should set processor_failure attribute
            set_attribute_calls = mock_span.set_attribute.call_args_list
            attributes = {call[0][0]: call[0][1] for call in set_attribute_calls}
            assert attributes.get("processor_failure") is True

    @pytest.mark.asyncio
    async def test_process_batch_handles_exception_during_processing(self, eventhub_consumer):
        """Test that _process_batch handles exceptions during processing."""
        from src.consumers.eventhub import MessageBatch, EventHubMessage
        from azure.eventhub import EventData
        
        batch = MessageBatch(max_size=1000, max_wait_seconds=60)
        
        # Add message
        mock_event = MagicMock(spec=EventData)
        mock_event.sequence_number = 100
        mock_event.offset = "200"
        mock_event.body = b"test"
        
        message = EventHubMessage(
            event_data=mock_event,
            partition_id="0",
            sequence_number=100
        )
        batch.add_message(message)
        
        # Mock message processor to raise exception
        eventhub_consumer.message_processor = MagicMock(
            side_effect=Exception("Processing failed")
        )
        
        with patch("src.consumers.eventhub.logfire") as mock_logfire, \
             patch("src.consumers.eventhub.logger") as mock_logger:
            
            mock_span = MagicMock()
            mock_logfire.span.return_value.__enter__ = MagicMock(return_value=mock_span)
            mock_logfire.span.return_value.__exit__ = MagicMock(return_value=False)
            
            # Should not raise
            await eventhub_consumer._process_batch(batch)
            
            # Should log error
            error_calls = [str(call) for call in mock_logger.error.call_args_list]
            assert any("Error processing batch" in call for call in error_calls)
            
            # Should log to Logfire
            mock_logfire.error.assert_called_once()
            call_args = mock_logfire.error.call_args
            assert call_args[0][0] == "Batch processing error"
            
            # Should set error attribute
            set_attribute_calls = mock_span.set_attribute.call_args_list
            attributes = {call[0][0]: call[0][1] for call in set_attribute_calls}
            assert "error" in attributes


class TestBatchTimeoutHandler:
    """Test cases for _batch_timeout_handler background task."""

    @pytest.mark.asyncio
    async def test_batch_timeout_handler_starts_and_logs(self, eventhub_consumer):
        """Test that batch timeout handler starts and logs initial message."""
        eventhub_consumer.running = True
        
        from src.consumers.eventhub import MessageBatch
        eventhub_consumer.current_batch = MessageBatch(max_size=1000, max_wait_seconds=60)
        
        with patch("src.consumers.eventhub.logger") as mock_logger, \
             patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            
            # Make it run once then stop
            async def stop_after_first_sleep(*args):
                eventhub_consumer.running = False
            
            mock_sleep.side_effect = stop_after_first_sleep
            
            await eventhub_consumer._batch_timeout_handler()
            
            # Should log start message
            info_calls = [str(call) for call in mock_logger.info.call_args_list]
            assert any("Batch timeout handler started" in call for call in info_calls)

    @pytest.mark.asyncio
    async def test_batch_timeout_handler_periodic_logging(self, eventhub_consumer):
        """Test that batch timeout handler logs periodically every minute."""
        eventhub_consumer.running = True
        
        from src.consumers.eventhub import MessageBatch
        eventhub_consumer.current_batch = MessageBatch(max_size=1000, max_wait_seconds=60)
        
        with patch("src.consumers.eventhub.logger") as mock_logger, \
             patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            
            sleep_count = 0
            async def stop_after_6_sleeps(*args):
                nonlocal sleep_count
                sleep_count += 1
                if sleep_count >= 6:  # 6 checks = 1 minute
                    eventhub_consumer.running = False
            
            mock_sleep.side_effect = stop_after_6_sleeps
            
            await eventhub_consumer._batch_timeout_handler()
            
            # Should log timeout check at 60 seconds (check_count % 6 == 0)
            info_calls = [str(call) for call in mock_logger.info.call_args_list]
            assert any("Timeout check #6" in call for call in info_calls)

    @pytest.mark.asyncio
    async def test_batch_timeout_handler_processes_ready_batch(self, eventhub_consumer):
        """Test that batch timeout handler processes batches when ready."""
        eventhub_consumer.running = True
        
        from src.consumers.eventhub import MessageBatch, EventHubMessage
        from azure.eventhub import EventData
        
        # Create a batch that will be ready (is_ready returns True)
        batch = MessageBatch(max_size=1000, max_wait_seconds=1)  # 1 second timeout
        
        # Add a message
        mock_event = MagicMock(spec=EventData)
        mock_event.sequence_number = 100
        mock_event.offset = "200"
        mock_event.body = b"test"
        mock_event.properties = {}
        
        message = EventHubMessage(
            event_data=mock_event,
            partition_id="0",
            sequence_number=100
        )
        batch.add_message(message)
        
        eventhub_consumer.current_batch = batch
        
        # Mock _process_batch
        eventhub_consumer._process_batch = AsyncMock()
        
        with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep, \
             patch("time.time", return_value=batch.created_at + 2):  # Make batch old enough
            
            sleep_count = 0
            async def stop_after_first_check(*args):
                nonlocal sleep_count
                sleep_count += 1
                if sleep_count >= 1:
                    eventhub_consumer.running = False
            
            mock_sleep.side_effect = stop_after_first_check
            
            await eventhub_consumer._batch_timeout_handler()
            
            # Should have processed the batch
            eventhub_consumer._process_batch.assert_called_once()
            
            # Should have created a new batch
            assert eventhub_consumer.current_batch != batch

    @pytest.mark.asyncio
    async def test_batch_timeout_handler_handles_cancellation(self, eventhub_consumer):
        """Test that batch timeout handler handles task cancellation gracefully."""
        eventhub_consumer.running = True
        
        from src.consumers.eventhub import MessageBatch
        eventhub_consumer.current_batch = MessageBatch(max_size=1000, max_wait_seconds=60)
        
        with patch("src.consumers.eventhub.logger") as mock_logger, \
             patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            
            # Simulate cancellation
            mock_sleep.side_effect = asyncio.CancelledError()
            
            await eventhub_consumer._batch_timeout_handler()
            
            # Should log cancellation
            info_calls = [str(call) for call in mock_logger.info.call_args_list]
            assert any("Batch timeout handler cancelled" in call for call in info_calls)

    @pytest.mark.asyncio
    async def test_batch_timeout_handler_handles_errors(self, eventhub_consumer):
        """Test that batch timeout handler handles errors and continues running."""
        eventhub_consumer.running = True
        
        from src.consumers.eventhub import MessageBatch
        eventhub_consumer.current_batch = MessageBatch(max_size=1000, max_wait_seconds=60)
        
        with patch("src.consumers.eventhub.logger") as mock_logger, \
             patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            
            call_count = 0
            async def error_then_stop(*args):
                nonlocal call_count
                call_count += 1
                if call_count == 1:
                    raise Exception("Test error")
                else:
                    eventhub_consumer.running = False
            
            mock_sleep.side_effect = error_then_stop
            
            await eventhub_consumer._batch_timeout_handler()
            
            # Should log error
            error_calls = [str(call) for call in mock_logger.error.call_args_list]
            assert any("Error in batch timeout handler" in call for call in error_calls)


class TestCheckpointSave:
    """Test cases for save_checkpoint with Logfire instrumentation."""

    @pytest.fixture
    def checkpoint_manager(self, sample_eventhub_config):
        """Create a checkpoint manager for testing."""
        from src.consumers.eventhub import MotherDuckCheckpointManager
        from src.utils.motherduck import MotherDuckConnectionConfig
        
        motherduck_config = MotherDuckConnectionConfig(
            MOTHERDUCK_TOKEN="test_token",
            TARGET_DB="test_db",
            TARGET_SCHEMA="test_schema",
            TARGET_TABLE="test_table"
        )
        
        manager = MotherDuckCheckpointManager(
            eventhub_namespace=sample_eventhub_config.namespace,
            eventhub_name=sample_eventhub_config.name,
            target_db="test_db",
            target_schema="test_schema",
            target_table="test_table",
            motherduck_config=motherduck_config,
        )
        
        return manager

    @pytest.mark.asyncio
    async def test_save_checkpoint_successful(self, checkpoint_manager):
        """Test successful checkpoint save with Logfire span."""
        partition_checkpoints = {"0": 1000, "1": 2000}
        partition_metadata = {
            "0": {"sequence_number": 100},
            "1": {"sequence_number": 200}
        }
        
        with patch("src.consumers.eventhub.logfire") as mock_logfire, \
             patch("src.consumers.eventhub.logger") as mock_logger, \
             patch("utils.motherduck.insert_partition_checkpoint") as mock_insert:
            
            mock_span = MagicMock()
            mock_logfire.span.return_value.__enter__ = MagicMock(return_value=mock_span)
            mock_logfire.span.return_value.__exit__ = MagicMock(return_value=False)
            
            result = await checkpoint_manager.save_checkpoint(
                partition_checkpoints, partition_metadata
            )
            
            # Should return True
            assert result is True
            
            # Should create span with correct attributes
            mock_logfire.span.assert_called_once()
            span_call = mock_logfire.span.call_args
            assert span_call[0][0] == "eventhub.checkpoint_save"
            assert span_call[1]["partitions_count"] == 2
            assert "0" in span_call[1]["partition_ids"]
            assert "1" in span_call[1]["partition_ids"]
            
            # Should set success attributes
            set_attribute_calls = mock_span.set_attribute.call_args_list
            attributes = {call[0][0]: call[0][1] for call in set_attribute_calls}
            assert attributes["checkpoints_saved"] == 2
            assert attributes["success"] is True
            
            # Should call insert for each partition
            assert mock_insert.call_count == 2

    @pytest.mark.asyncio
    async def test_save_checkpoint_logs_partition_data(self, checkpoint_manager):
        """Test that save_checkpoint logs each partition's data."""
        partition_checkpoints = {"0": 1000}
        partition_metadata = {"0": {"sequence_number": 100, "timestamp": "2025-10-24"}}
        
        with patch("src.consumers.eventhub.logfire") as mock_logfire, \
             patch("src.consumers.eventhub.logger") as mock_logger, \
             patch("utils.motherduck.insert_partition_checkpoint"):
            
            mock_span = MagicMock()
            mock_logfire.span.return_value.__enter__ = MagicMock(return_value=mock_span)
            mock_logfire.span.return_value.__exit__ = MagicMock(return_value=False)
            
            await checkpoint_manager.save_checkpoint(
                partition_checkpoints, partition_metadata
            )
            
            # Should log insertion details
            info_calls = [str(call) for call in mock_logger.info.call_args_list]
            assert any("📝 Inserting checkpoint" in call for call in info_calls)
            assert any("partition=0" in call for call in info_calls)
            assert any("waterlevel=1000" in call for call in info_calls)

    @pytest.mark.asyncio
    async def test_save_checkpoint_handles_errors(self, checkpoint_manager):
        """Test that save_checkpoint handles errors gracefully."""
        partition_checkpoints = {"0": 1000}
        
        with patch("src.consumers.eventhub.logfire") as mock_logfire, \
             patch("src.consumers.eventhub.logger") as mock_logger, \
             patch("utils.motherduck.insert_partition_checkpoint", 
                   side_effect=Exception("Database error")):
            
            mock_span = MagicMock()
            mock_logfire.span.return_value.__enter__ = MagicMock(return_value=mock_span)
            mock_logfire.span.return_value.__exit__ = MagicMock(return_value=False)
            
            result = await checkpoint_manager.save_checkpoint(partition_checkpoints)
            
            # Should return False
            assert result is False
            
            # Should log error
            error_calls = [str(call) for call in mock_logger.error.call_args_list]
            assert any("Failed to save checkpoint" in call for call in error_calls)
            
            # Should set error attributes
            set_attribute_calls = mock_span.set_attribute.call_args_list
            attributes = {call[0][0]: call[0][1] for call in set_attribute_calls}
            assert "error" in attributes
            assert attributes["success"] is False
            
            # Should log to Logfire
            mock_logfire.error.assert_called_once()
            call_args = mock_logfire.error.call_args
            assert call_args[0][0] == "Checkpoint save failed"

    @pytest.mark.asyncio
    async def test_save_checkpoint_without_metadata(self, checkpoint_manager):
        """Test save_checkpoint works without metadata."""
        partition_checkpoints = {"0": 1000, "1": 2000}
        
        with patch("src.consumers.eventhub.logfire") as mock_logfire, \
             patch("src.consumers.eventhub.logger"), \
             patch("utils.motherduck.insert_partition_checkpoint") as mock_insert:
            
            mock_span = MagicMock()
            mock_logfire.span.return_value.__enter__ = MagicMock(return_value=mock_span)
            mock_logfire.span.return_value.__exit__ = MagicMock(return_value=False)
            
            result = await checkpoint_manager.save_checkpoint(partition_checkpoints)
            
            # Should return True
            assert result is True
            
            # Should call insert with None metadata
            for call in mock_insert.call_args_list:
                assert call[1].get("metadata") is None or call[1].get("metadata") == {}

    @pytest.mark.asyncio
    async def test_save_checkpoint_logs_success_to_logfire(self, checkpoint_manager):
        """Test that save_checkpoint logs success to Logfire."""
        partition_checkpoints = {"0": 1000}
        
        with patch("src.consumers.eventhub.logfire") as mock_logfire, \
             patch("src.consumers.eventhub.logger"), \
             patch("utils.motherduck.insert_partition_checkpoint"):
            
            mock_span = MagicMock()
            mock_logfire.span.return_value.__enter__ = MagicMock(return_value=mock_span)
            mock_logfire.span.return_value.__exit__ = MagicMock(return_value=False)
            
            await checkpoint_manager.save_checkpoint(partition_checkpoints)
            
            # Should log success to Logfire
            mock_logfire.info.assert_called_once()
            call_args = mock_logfire.info.call_args
            assert call_args[0][0] == "Checkpoints saved to MotherDuck"
            assert call_args[1]["partitions_count"] == 1


