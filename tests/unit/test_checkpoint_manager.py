"""
Unit tests for MotherDuckCheckpointManager (from src/consumers/eventhub.py).

Tests cover:
- Checkpoint retrieval (with/without existing checkpoints)
- Checkpoint saving with per-partition tracking
- Error handling and logging
- Metadata preservation
"""

from typing import Dict, Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.consumers.eventhub import MotherDuckCheckpointManager
from src.utils.config import MotherDuckConfig
from src.utils.motherduck import MotherDuckConnectionConfig


# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture
def checkpoint_manager(sample_motherduck_config: MotherDuckConfig):
    """Create a checkpoint manager instance for testing."""
    return MotherDuckCheckpointManager(
        eventhub_namespace="eventhu1.servicebus.windows.net",
        eventhub_name="topic1",
        target_db="mother_ducklake",
        target_schema="ingest",
        target_table="table1",
        motherduck_config=None,
        session=None,
    )


@pytest.fixture
def mock_md_session():
    """Create a mock MotherDuck session."""
    mock = MagicMock()
    mock.execute = MagicMock()
    mock.close = MagicMock()
    return mock


# ============================================================================
# MotherDuckCheckpointManager Initialization
# ============================================================================


class TestCheckpointManagerInitialization:
    """Test checkpoint manager initialization."""

    def test_init_with_all_parameters(
        self, sample_motherduck_config: MotherDuckConfig, mock_md_session
    ):
        """Test initializing checkpoint manager with all parameters."""
        manager = MotherDuckCheckpointManager(
            eventhub_namespace="eventhu1.servicebus.windows.net",
            eventhub_name="topic1",
            target_db="mother_ducklake",
            target_schema="ingest",
            target_table="table1",
            motherduck_config=sample_motherduck_config,
            session=mock_md_session,
        )

        assert manager.eventhub_namespace == "eventhu1.servicebus.windows.net"
        assert manager.eventhub_name == "topic1"
        assert manager.target_db == "mother_ducklake"
        assert manager.target_schema == "ingest"
        assert manager.target_table == "table1"
        assert manager.motherduck_config == sample_motherduck_config
        assert manager.session == mock_md_session
        assert manager._external_session is True

    def test_init_without_session(self, checkpoint_manager):
        """Test initializing checkpoint manager without external session."""
        assert checkpoint_manager._external_session is False
        assert checkpoint_manager.session is None

    def test_init_with_session(self, checkpoint_manager, mock_md_session):
        """Test initializing checkpoint manager with external session."""
        manager = MotherDuckCheckpointManager(
            eventhub_namespace="eventhu1.servicebus.windows.net",
            eventhub_name="topic1",
            target_db="mother_ducklake",
            target_schema="ingest",
            target_table="table1",
            session=mock_md_session,
        )
        assert manager._external_session is True
        assert manager.session == mock_md_session


# ============================================================================
# Checkpoint Retrieval Tests
# ============================================================================


class TestCheckpointRetrieval:
    """Test checkpoint retrieval from MotherDuck."""

    @pytest.mark.asyncio
    async def test_get_last_checkpoint_returns_checkpoints(self, checkpoint_manager):
        """Test retrieving existing checkpoints."""
        expected_checkpoints = {"0": 100, "1": 150, "2": 120}

        with patch(
            "utils.motherduck.get_partition_checkpoints"
        ) as mock_get:
            mock_get.return_value = expected_checkpoints

            result = await checkpoint_manager.get_last_checkpoint()

            assert result == expected_checkpoints
            mock_get.assert_called_once_with(
                eventhub_namespace="eventhu1.servicebus.windows.net",
                eventhub="topic1",
                target_db="mother_ducklake",
                target_schema="ingest",
                target_table="table1",
                conn=None,
                config=None,
            )

    @pytest.mark.asyncio
    async def test_get_last_checkpoint_no_checkpoints(self, checkpoint_manager):
        """Test retrieving when no checkpoints exist."""
        with patch(
            "utils.motherduck.get_partition_checkpoints"
        ) as mock_get:
            mock_get.return_value = None

            result = await checkpoint_manager.get_last_checkpoint()

            assert result is None
            mock_get.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_last_checkpoint_empty_dict(self, checkpoint_manager):
        """Test retrieving empty checkpoint dictionary."""
        with patch(
            "utils.motherduck.get_partition_checkpoints"
        ) as mock_get:
            mock_get.return_value = {}

            result = await checkpoint_manager.get_last_checkpoint()

            assert result == {}

    @pytest.mark.asyncio
    async def test_get_last_checkpoint_error_handling(self, checkpoint_manager):
        """Test error handling during checkpoint retrieval."""
        with patch(
            "utils.motherduck.get_partition_checkpoints"
        ) as mock_get:
            mock_get.side_effect = Exception("Database connection failed")

            result = await checkpoint_manager.get_last_checkpoint()

            assert result is None

    @pytest.mark.asyncio
    async def test_get_last_checkpoint_with_session(
        self, checkpoint_manager, mock_md_session
    ):
        """Test checkpoint retrieval with external session."""
        checkpoint_manager.session = mock_md_session
        expected_checkpoints = {"0": 200}

        with patch(
            "utils.motherduck.get_partition_checkpoints"
        ) as mock_get:
            mock_get.return_value = expected_checkpoints

            result = await checkpoint_manager.get_last_checkpoint()

            assert result == expected_checkpoints
            # Verify session was passed as conn
            mock_get.assert_called_once()
            call_kwargs = mock_get.call_args[1]
            assert call_kwargs["conn"] == mock_md_session


# ============================================================================
# Checkpoint Saving Tests
# ============================================================================


class TestCheckpointSaving:
    """Test checkpoint saving to MotherDuck."""

    @pytest.mark.asyncio
    async def test_save_checkpoint_single_partition(self, checkpoint_manager):
        """Test saving checkpoint for single partition."""
        partition_checkpoints = {"0": 100}

        with patch(
            "utils.motherduck.insert_partition_checkpoint"
        ) as mock_insert:
            result = await checkpoint_manager.save_checkpoint(
                partition_checkpoints
            )

            assert result is True
            mock_insert.assert_called_once()
            call_kwargs = mock_insert.call_args[1]
            assert call_kwargs["partition_id"] == "0"
            assert call_kwargs["waterlevel"] == 100

    @pytest.mark.asyncio
    async def test_save_checkpoint_multiple_partitions(self, checkpoint_manager):
        """Test saving checkpoints for multiple partitions."""
        partition_checkpoints = {"0": 100, "1": 150, "2": 120}

        with patch(
            "utils.motherduck.insert_partition_checkpoint"
        ) as mock_insert:
            result = await checkpoint_manager.save_checkpoint(
                partition_checkpoints
            )

            assert result is True
            assert mock_insert.call_count == 3

            # Verify all partitions were saved
            call_partition_ids = [
                call[1]["partition_id"] for call in mock_insert.call_args_list
            ]
            assert set(call_partition_ids) == {"0", "1", "2"}

    @pytest.mark.asyncio
    async def test_save_checkpoint_with_metadata(self, checkpoint_manager):
        """Test saving checkpoints with partition metadata."""
        partition_checkpoints = {"0": 100, "1": 150}
        partition_metadata = {
            "0": {"sequence_number": 100, "timestamp": "2025-10-17T20:00:00Z"},
            "1": {"sequence_number": 150, "timestamp": "2025-10-17T20:01:00Z"},
        }

        with patch(
            "utils.motherduck.insert_partition_checkpoint"
        ) as mock_insert:
            result = await checkpoint_manager.save_checkpoint(
                partition_checkpoints, partition_metadata
            )

            assert result is True
            assert mock_insert.call_count == 2

            # Verify metadata was passed
            for call in mock_insert.call_args_list:
                call_kwargs = call[1]
                partition_id = call_kwargs["partition_id"]
                assert call_kwargs["metadata"] == partition_metadata[partition_id]

    @pytest.mark.asyncio
    async def test_save_checkpoint_partial_metadata(self, checkpoint_manager):
        """Test saving checkpoints with partial metadata."""
        partition_checkpoints = {"0": 100, "1": 150}
        partition_metadata = {
            "0": {"sequence_number": 100},  # Only partition 0 has metadata
        }

        with patch(
            "utils.motherduck.insert_partition_checkpoint"
        ) as mock_insert:
            result = await checkpoint_manager.save_checkpoint(
                partition_checkpoints, partition_metadata
            )

            assert result is True
            assert mock_insert.call_count == 2

            # Verify partition 0 has metadata
            call_0 = mock_insert.call_args_list[0]
            call_kwargs_0 = call_0[1]
            if call_kwargs_0["partition_id"] == "0":
                assert call_kwargs_0["metadata"] is not None
            else:
                assert call_kwargs_0["metadata"] is None

    @pytest.mark.asyncio
    async def test_save_checkpoint_empty_dict(self, checkpoint_manager):
        """Test saving empty checkpoint dictionary."""
        partition_checkpoints = {}

        with patch(
            "utils.motherduck.insert_partition_checkpoint"
        ) as mock_insert:
            result = await checkpoint_manager.save_checkpoint(
                partition_checkpoints
            )

            assert result is True
            # No inserts should be called for empty dict
            mock_insert.assert_not_called()

    @pytest.mark.asyncio
    async def test_save_checkpoint_error_handling(self, checkpoint_manager):
        """Test error handling during checkpoint saving."""
        partition_checkpoints = {"0": 100}

        with patch(
            "utils.motherduck.insert_partition_checkpoint"
        ) as mock_insert:
            mock_insert.side_effect = Exception("Insert failed")

            result = await checkpoint_manager.save_checkpoint(
                partition_checkpoints
            )

            assert result is False

    @pytest.mark.asyncio
    async def test_save_checkpoint_preserves_parameters(self, checkpoint_manager):
        """Test that saved checkpoints preserve all parameters."""
        partition_checkpoints = {"5": 999}

        with patch(
            "utils.motherduck.insert_partition_checkpoint"
        ) as mock_insert:
            result = await checkpoint_manager.save_checkpoint(
                partition_checkpoints
            )

            assert result is True
            call_kwargs = mock_insert.call_args[1]

            # Verify all manager parameters are passed
            assert call_kwargs["eventhub_namespace"] == "eventhu1.servicebus.windows.net"
            assert call_kwargs["eventhub"] == "topic1"
            assert call_kwargs["target_db"] == "mother_ducklake"
            assert call_kwargs["target_schema"] == "ingest"
            assert call_kwargs["target_table"] == "table1"


# ============================================================================
# Checkpoint Manager Integration Tests
# ============================================================================


class TestCheckpointManagerIntegration:
    """Test checkpoint manager integration scenarios."""

    @pytest.mark.asyncio
    async def test_get_and_save_cycle(self, checkpoint_manager):
        """Test getting and saving checkpoints in sequence."""
        initial_checkpoints = {"0": 100, "1": 150}
        updated_checkpoints = {"0": 200, "1": 175}

        with patch(
            "utils.motherduck.get_partition_checkpoints"
        ) as mock_get, patch(
            "utils.motherduck.insert_partition_checkpoint"
        ) as mock_insert:
            # First, get initial checkpoints
            mock_get.return_value = initial_checkpoints
            result1 = await checkpoint_manager.get_last_checkpoint()
            assert result1 == initial_checkpoints

            # Then save updated checkpoints
            result2 = await checkpoint_manager.save_checkpoint(
                updated_checkpoints
            )
            assert result2 is True
            assert mock_insert.call_count == 2

    @pytest.mark.asyncio
    async def test_multiple_checkpoint_operations(self, checkpoint_manager):
        """Test performing multiple checkpoint operations."""
        checkpoints_1 = {"0": 100}
        checkpoints_2 = {"1": 200}
        checkpoints_3 = {"0": 150, "1": 250}

        with patch(
            "utils.motherduck.insert_partition_checkpoint"
        ) as mock_insert:
            # Save multiple batches
            await checkpoint_manager.save_checkpoint(checkpoints_1)
            await checkpoint_manager.save_checkpoint(checkpoints_2)
            await checkpoint_manager.save_checkpoint(checkpoints_3)

            # Total calls: 1 + 1 + 2 = 4
            assert mock_insert.call_count == 4

    @pytest.mark.asyncio
    async def test_checkpoint_with_large_sequence_numbers(
        self, checkpoint_manager
    ):
        """Test checkpoints with large sequence numbers."""
        large_checkpoints = {
            "0": 9223372036854775807,  # Max int64
            "1": 1000000000,
        }

        with patch(
            "utils.motherduck.insert_partition_checkpoint"
        ) as mock_insert:
            result = await checkpoint_manager.save_checkpoint(
                large_checkpoints
            )

            assert result is True
            # Verify large numbers are preserved
            call_waterlevels = [
                call[1]["waterlevel"] for call in mock_insert.call_args_list
            ]
            assert 9223372036854775807 in call_waterlevels
            assert 1000000000 in call_waterlevels


# ============================================================================
# Edge Cases and Boundary Conditions
# ============================================================================


class TestCheckpointManagerEdgeCases:
    """Test edge cases and boundary conditions."""

    @pytest.mark.asyncio
    async def test_checkpoint_with_string_partition_ids(self, checkpoint_manager):
        """Test checkpoints with various partition ID formats."""
        checkpoints = {
            "0": 100,
            "999": 200,
            "partition-0": 300,
        }

        with patch(
            "utils.motherduck.insert_partition_checkpoint"
        ) as mock_insert:
            result = await checkpoint_manager.save_checkpoint(checkpoints)

            assert result is True
            assert mock_insert.call_count == 3

    @pytest.mark.asyncio
    async def test_checkpoint_zero_waterlevel(self, checkpoint_manager):
        """Test checkpoint with waterlevel of 0."""
        checkpoints = {"0": 0}

        with patch(
            "utils.motherduck.insert_partition_checkpoint"
        ) as mock_insert:
            result = await checkpoint_manager.save_checkpoint(checkpoints)

            assert result is True
            call_kwargs = mock_insert.call_args[1]
            assert call_kwargs["waterlevel"] == 0

    @pytest.mark.asyncio
    async def test_none_metadata_dict(self, checkpoint_manager):
        """Test saving with None metadata dictionary."""
        checkpoints = {"0": 100}

        with patch(
            "utils.motherduck.insert_partition_checkpoint"
        ) as mock_insert:
            result = await checkpoint_manager.save_checkpoint(
                checkpoints, partition_metadata=None
            )

            assert result is True
            call_kwargs = mock_insert.call_args[1]
            assert call_kwargs["metadata"] is None
