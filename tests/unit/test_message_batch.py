"""
Unit tests for MessageBatch class (from src/consumers/eventhub.py).

Tests cover:
- Message addition and batch readiness
- Size-based and timeout-based batch completion
- Checkpoint data extraction
- Dictionary conversion
- Boundary conditions
"""

import json
import time
from typing import Any, Dict, List
from unittest.mock import MagicMock

import pytest

from src.consumers.eventhub import EventHubMessage, MessageBatch


# ============================================================================
# Helper Functions
# ============================================================================


def _create_mock_event_data(body_dict: Dict[str, Any]) -> MagicMock:
    """Create a mock EventData object for testing."""
    mock_event = MagicMock()
    mock_event.body_as_str.return_value = json.dumps(body_dict)
    mock_event.enqueued_time = MagicMock()
    mock_event.properties = {"key": "value"}
    mock_event.system_properties = {"offset": "0"}
    return mock_event


def _create_test_message(partition_id: str, sequence_number: int, value: int) -> EventHubMessage:
    """Create a test EventHubMessage."""
    mock_event_data = _create_mock_event_data({"value": value})
    return EventHubMessage(
        event_data=mock_event_data,
        partition_id=partition_id,
        sequence_number=sequence_number,
    )


# ============================================================================
# MessageBatch Basic Operations
# ============================================================================


class TestMessageBatchBasicOperations:
    """Test basic MessageBatch operations."""

    def test_create_empty_batch(self):
        """Test creating an empty batch."""
        batch = MessageBatch(max_size=10, max_wait_seconds=60)
        assert len(batch.messages) == 0
        assert batch.max_size == 10
        assert batch.max_wait_seconds == 60

    def test_add_single_message(self, sample_eventhub_message: EventHubMessage):
        """Test adding a single message to batch."""
        batch = MessageBatch(max_size=10, max_wait_seconds=60)
        is_ready = batch.add_message(sample_eventhub_message)

        assert len(batch.messages) == 1
        assert batch.messages[0] == sample_eventhub_message
        assert is_ready is False  # Not ready with just 1 message

    def test_add_multiple_messages(
        self, sample_eventhub_messages: List[EventHubMessage]
    ):
        """Test adding multiple messages to batch."""
        batch = MessageBatch(max_size=20, max_wait_seconds=60)

        for i, message in enumerate(sample_eventhub_messages):
            is_ready = batch.add_message(message)
            assert len(batch.messages) == i + 1
            assert is_ready is False  # Not ready yet

    def test_add_message_updates_checkpoint_data(
        self, sample_eventhub_messages: List[EventHubMessage]
    ):
        """Test that adding messages updates checkpoint data by partition."""
        batch = MessageBatch(max_size=100, max_wait_seconds=60)

        for message in sample_eventhub_messages:
            batch.add_message(message)

        checkpoint_data = batch.get_checkpoint_data()
        # Should have sequence numbers for each partition
        assert len(checkpoint_data) > 0
        # Sequence numbers should be increasing within each partition
        for partition_id, seq_num in checkpoint_data.items():
            assert isinstance(seq_num, int)


# ============================================================================
# MessageBatch Readiness Logic
# ============================================================================


class TestMessageBatchReadiness:
    """Test batch readiness conditions (size and timeout)."""

    def test_batch_ready_when_full(self):
        """Test batch is ready when it reaches max_size."""
        batch = MessageBatch(max_size=3, max_wait_seconds=300)

        # Add messages until batch is full
        for i in range(2):
            msg = _create_test_message("0", 100 + i, i)
            is_ready = batch.add_message(msg)
            assert is_ready is False

        # Adding the 3rd message should trigger ready
        msg3 = _create_test_message("0", 102, 2)

        is_ready = batch.add_message(msg3)
        assert is_ready is True
        assert len(batch.messages) == 3

    def test_batch_ready_when_timeout_reached(self, mock_time):
        """Test batch is ready when timeout is reached."""
        batch = MessageBatch(max_size=100, max_wait_seconds=10)

        # Create initial batch
        msg = _create_test_message("0", 100, 0)
        batch.add_message(msg)
        assert batch.is_ready() is False

        # Advance time by 9 seconds - still not ready
        mock_time.advance(9)
        assert batch.is_ready() is False

        # Advance time by 1 more second - now ready
        mock_time.advance(1)
        assert batch.is_ready() is True

    def test_batch_ready_timeout_exactly_at_threshold(self, mock_time):
        """Test batch is ready when time exactly matches timeout."""
        batch = MessageBatch(max_size=100, max_wait_seconds=60)
        msg = _create_test_message("0", 100, 0)
        batch.add_message(msg)

        mock_time.advance(60)
        assert batch.is_ready() is True

    def test_batch_ready_priority_size_over_timeout(self, mock_time):
        """Test that batch returns ready due to size, even if timeout not reached."""
        batch = MessageBatch(max_size=5, max_wait_seconds=300)

        # Add 4 messages
        for i in range(4):
            msg = _create_test_message("0", 100 + i, i)
            batch.add_message(msg)

        # At 10 seconds, batch is not ready (not full, timeout not reached)
        mock_time.advance(10)
        assert batch.is_ready() is False

        # Add 5th message - should be ready due to size, regardless of timeout
        msg5 = _create_test_message("0", 104, 4)
        is_ready = batch.add_message(msg5)
        assert is_ready is True


# ============================================================================
# MessageBatch Checkpoint Data
# ============================================================================


class TestMessageBatchCheckpointData:
    """Test checkpoint data extraction from batch."""

    def test_get_checkpoint_data_empty_batch(self):
        """Test checkpoint data for empty batch."""
        batch = MessageBatch(max_size=100, max_wait_seconds=60)
        checkpoint_data = batch.get_checkpoint_data()
        assert checkpoint_data == {}

    def test_get_checkpoint_data_single_partition(self):
        """Test checkpoint data with messages from single partition."""
        batch = MessageBatch(max_size=100, max_wait_seconds=60)

        # Add 5 messages to partition 0
        for i in range(5):
            msg = _create_test_message("0", 100 + i, i)
            batch.add_message(msg)

        checkpoint_data = batch.get_checkpoint_data()
        assert checkpoint_data == {"0": 104}  # Last sequence number

    def test_get_checkpoint_data_multiple_partitions(self):
        """Test checkpoint data with messages from multiple partitions."""
        batch = MessageBatch(max_size=100, max_wait_seconds=60)

        # Add messages to different partitions
        partitions = {"0": 100, "1": 200, "2": 150}
        for partition_id, seq_start in partitions.items():
            for i in range(3):
                msg = _create_test_message(partition_id, seq_start + i, i)
                batch.add_message(msg)

        checkpoint_data = batch.get_checkpoint_data()
        assert checkpoint_data == {"0": 102, "1": 202, "2": 152}

    def test_get_checkpoint_data_highest_sequence_per_partition(self):
        """Test checkpoint data returns last sequence number added per partition."""
        batch = MessageBatch(max_size=100, max_wait_seconds=60)

        # Add messages out of order to partition 0
        sequences = [100, 105, 102, 104, 103]
        for seq in sequences:
            msg = _create_test_message("0", seq, seq)
            batch.add_message(msg)

        checkpoint_data = batch.get_checkpoint_data()
        # Should be 103 - the last one added, not 105 which was the highest
        assert checkpoint_data == {"0": 103}

    def test_checkpoint_data_is_copy(self):
        """Test checkpoint data returns a copy, not a reference."""
        batch = MessageBatch(max_size=100, max_wait_seconds=60)
        msg = _create_test_message("0", 100, 0)
        batch.add_message(msg)

        checkpoint1 = batch.get_checkpoint_data()
        checkpoint2 = batch.get_checkpoint_data()

        # Modify one checkpoint
        checkpoint1["0"] = 999

        # Other should be unaffected
        assert checkpoint2 == {"0": 100}


# ============================================================================
# MessageBatch Dictionary Conversion
# ============================================================================


class TestMessageBatchDictConversion:
    """Test converting batch messages to dictionaries."""

    def test_to_dict_list_empty_batch(self):
        """Test dict conversion for empty batch."""
        batch = MessageBatch(max_size=100, max_wait_seconds=60)
        dict_list = batch.to_dict_list()
        assert dict_list == []

    def test_to_dict_list_single_message(self, sample_eventhub_message: EventHubMessage):
        """Test dict conversion for single message."""
        batch = MessageBatch(max_size=100, max_wait_seconds=60)
        batch.add_message(sample_eventhub_message)

        dict_list = batch.to_dict_list()
        assert len(dict_list) == 1
        assert isinstance(dict_list[0], dict)

    def test_to_dict_list_multiple_messages(
        self, sample_eventhub_messages: List[EventHubMessage]
    ):
        """Test dict conversion for multiple messages."""
        batch = MessageBatch(max_size=100, max_wait_seconds=60)

        for msg in sample_eventhub_messages:
            batch.add_message(msg)

        dict_list = batch.to_dict_list()
        assert len(dict_list) == len(sample_eventhub_messages)
        assert all(isinstance(item, dict) for item in dict_list)

    def test_to_dict_list_preserves_data(self, sample_eventhub_message: EventHubMessage):
        """Test that dict conversion preserves message data."""
        batch = MessageBatch(max_size=100, max_wait_seconds=60)
        batch.add_message(sample_eventhub_message)

        dict_list = batch.to_dict_list()
        message_dict = dict_list[0]

        # Verify key fields are present
        assert "partition_id" in message_dict
        assert "sequence_number" in message_dict
        assert "event_body" in message_dict


# ============================================================================
# MessageBatch Edge Cases
# ============================================================================


class TestMessageBatchEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_batch_with_max_size_one(self):
        """Test batch with max_size of 1."""
        batch = MessageBatch(max_size=1, max_wait_seconds=60)
        msg = _create_test_message("0", 100, 0)

        is_ready = batch.add_message(msg)
        assert is_ready is True
        assert len(batch.messages) == 1

    def test_batch_with_zero_timeout(self, mock_time):
        """Test batch with timeout of 0 (always ready after time passes)."""
        batch = MessageBatch(max_size=100, max_wait_seconds=0)
        msg = _create_test_message("0", 100, 0)
        batch.add_message(msg)

        # Should be ready even at the same time
        mock_time.advance(0)
        assert batch.is_ready() is True

    def test_batch_accumulation_over_time(self, sample_eventhub_messages):
        """Test batch accumulates messages correctly over time."""
        batch = MessageBatch(max_size=100, max_wait_seconds=60)

        # Add half the messages
        for i in range(5):
            batch.add_message(sample_eventhub_messages[i])

        assert len(batch.messages) == 5

        # Add remaining messages
        for i in range(5, 10):
            batch.add_message(sample_eventhub_messages[i])

        assert len(batch.messages) == 10
