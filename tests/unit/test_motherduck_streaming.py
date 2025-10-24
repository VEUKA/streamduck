"""
Unit tests for MotherDuckStreamingClient (from src/streaming/motherduck.py).

Tests cover:
- Client initialization and lifecycle
- Connection management
- Table creation and schema management
- Batch ingestion
- Statistics tracking
- Health checks
- Error handling and recovery
"""

from datetime import datetime, timezone
from typing import Any, Dict, List
from unittest.mock import MagicMock, Mock, patch

import pytest

from src.streaming.motherduck import MotherDuckStreamingClient
from src.utils.config import MotherDuckConfig


# ============================================================================
# Fixtures
# ============================================================================


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
def mock_duckdb_connection():
    """Create a mock DuckDB connection."""
    mock_conn = MagicMock()
    mock_conn.execute = MagicMock(return_value=MagicMock(fetchone=MagicMock(return_value=(1,))))
    mock_conn.close = MagicMock()
    return mock_conn


@pytest.fixture
def streaming_client(sample_motherduck_config):
    """Create a MotherDuckStreamingClient instance for testing."""
    return MotherDuckStreamingClient(
        motherduck_config=sample_motherduck_config,
        connection_config=None,
        client_name_suffix="test-client",
    )


# ============================================================================
# Initialization Tests
# ============================================================================


class TestMotherDuckStreamingClientInitialization:
    """Test MotherDuckStreamingClient initialization."""

    def test_init_with_valid_config(self, sample_motherduck_config):
        """Test initializing client with valid configuration."""
        client = MotherDuckStreamingClient(
            motherduck_config=sample_motherduck_config,
            connection_config=None,
            client_name_suffix="test-suffix",
        )

        assert client.motherduck_config == sample_motherduck_config
        assert client.client_name_suffix == "test-suffix"
        assert client.conn is None
        assert client.stats["total_messages_sent"] == 0
        assert client.stats["total_batches_sent"] == 0

    def test_init_generates_default_suffix(self, sample_motherduck_config):
        """Test that client generates a UUID suffix if none provided."""
        client = MotherDuckStreamingClient(
            motherduck_config=sample_motherduck_config,
        )

        assert client.client_name_suffix is not None
        assert len(client.client_name_suffix) == 8

    def test_init_stats_structure(self, sample_motherduck_config):
        """Test that stats dictionary is properly initialized."""
        client = MotherDuckStreamingClient(
            motherduck_config=sample_motherduck_config,
        )

        assert "client_created_at" in client.stats
        assert "total_messages_sent" in client.stats
        assert "total_batches_sent" in client.stats
        assert "last_ingestion" in client.stats
        assert client.stats["client_created_at"] is None


# ============================================================================
# Connection & Lifecycle Tests
# ============================================================================


class TestMotherDuckStreamingClientConnection:
    """Test client connection management."""

    def test_client_connection_initially_none(self, streaming_client):
        """Test that connection is None initially."""
        assert streaming_client.conn is None

    @patch("src.streaming.motherduck.duckdb.connect")
    def test_start_creates_connection(self, mock_duckdb_connect, streaming_client, mock_duckdb_connection):
        """Test that start() creates a DuckDB connection."""
        mock_duckdb_connect.return_value = mock_duckdb_connection

        streaming_client.start()

        assert streaming_client.conn is not None
        assert streaming_client.conn == mock_duckdb_connection
        mock_duckdb_connect.assert_called_once()

    @patch("src.streaming.motherduck.duckdb.connect")
    def test_start_sets_creation_timestamp(self, mock_duckdb_connect, streaming_client, mock_duckdb_connection):
        """Test that start() sets the client creation timestamp."""
        mock_duckdb_connect.return_value = mock_duckdb_connection
        before_start = datetime.now(timezone.utc)

        streaming_client.start()

        after_start = datetime.now(timezone.utc)
        assert streaming_client.stats["client_created_at"] is not None
        assert before_start <= streaming_client.stats["client_created_at"] <= after_start

    def test_stop_closes_connection(self, streaming_client, mock_duckdb_connection):
        """Test that stop() closes the connection."""
        streaming_client.conn = mock_duckdb_connection

        streaming_client.stop()

        mock_duckdb_connection.close.assert_called_once()
        assert streaming_client.conn is None

    def test_stop_without_connection(self, streaming_client):
        """Test that stop() gracefully handles no connection."""
        streaming_client.conn = None
        streaming_client.stop()  # Should not raise
        assert streaming_client.conn is None

    @patch("src.streaming.motherduck.duckdb.connect")
    def test_start_already_running(self, mock_duckdb_connect, streaming_client, mock_duckdb_connection):
        """Test that start() doesn't reconnect if already running."""
        mock_duckdb_connect.return_value = mock_duckdb_connection
        streaming_client.conn = mock_duckdb_connection

        streaming_client.start()

        # Should log warning but not call connect again
        assert streaming_client.conn == mock_duckdb_connection


# ============================================================================
# Table Management Tests
# ============================================================================


class TestMotherDuckStreamingClientTableManagement:
    """Test table and schema creation."""

    @patch("src.streaming.motherduck.duckdb.connect")
    def test_start_creates_schema_and_table(
        self, mock_duckdb_connect, streaming_client, mock_duckdb_connection
    ):
        """Test that start() creates schema and table."""
        mock_duckdb_connect.return_value = mock_duckdb_connection

        streaming_client.start()

        # Should have called execute for schema and table creation
        assert mock_duckdb_connection.execute.call_count >= 2

    def test_ensure_target_table_no_connection(self, streaming_client):
        """Test that _ensure_target_table raises if no connection."""
        streaming_client.conn = None

        with pytest.raises(RuntimeError, match="Connection not established"):
            streaming_client._ensure_target_table()

    def test_ensure_target_table_creates_schema(self, streaming_client, mock_duckdb_connection):
        """Test that _ensure_target_table creates schema."""
        streaming_client.conn = mock_duckdb_connection

        streaming_client._ensure_target_table()

        # Verify execute was called with CREATE SCHEMA
        calls = [call[0][0] for call in mock_duckdb_connection.execute.call_args_list]
        assert any("CREATE SCHEMA" in call for call in calls)

    def test_ensure_target_table_creates_table(self, streaming_client, mock_duckdb_connection):
        """Test that _ensure_target_table creates table."""
        streaming_client.conn = mock_duckdb_connection

        streaming_client._ensure_target_table()

        # Verify execute was called with CREATE TABLE
        calls = [call[0][0] for call in mock_duckdb_connection.execute.call_args_list]
        assert any("CREATE TABLE" in call for call in calls)


# ============================================================================
# Batch Ingestion Tests
# ============================================================================


class TestMotherDuckStreamingClientBatchIngestion:
    """Test batch ingestion functionality."""

    def test_ingest_batch_no_connection(self, streaming_client):
        """Test that ingest_batch returns False without connection."""
        streaming_client.conn = None
        test_batch = [{"event_body": "test", "partition_id": "0", "sequence_number": 1}]

        # Should return False when client is not started (error is caught and logged)
        result = streaming_client.ingest_batch("test-channel", test_batch)
        assert result is False

    def test_ingest_batch_empty_batch(self, streaming_client, mock_duckdb_connection):
        """Test that ingest_batch handles empty batches gracefully."""
        streaming_client.conn = mock_duckdb_connection

        result = streaming_client.ingest_batch("test-channel", [])

        assert result is True
        # Should not execute INSERT for empty batch
        mock_duckdb_connection.execute.assert_not_called()

    def test_ingest_batch_single_record(self, streaming_client, mock_duckdb_connection):
        """Test ingesting a single record."""
        streaming_client.conn = mock_duckdb_connection
        test_batch = [
            {
                "event_body": "test_message",
                "partition_id": "0",
                "sequence_number": 100,
                "enqueued_time": "2025-10-17T12:00:00Z",
                "properties": None,
                "system_properties": None,
                "ingestion_timestamp": "2025-10-17T12:00:01Z",
            }
        ]

        result = streaming_client.ingest_batch("test-channel", test_batch)

        assert result is True
        assert streaming_client.stats["total_messages_sent"] == 1
        assert streaming_client.stats["total_batches_sent"] == 1
        mock_duckdb_connection.execute.assert_called_once()

    def test_ingest_batch_multiple_records(self, streaming_client, mock_duckdb_connection):
        """Test ingesting multiple records in a batch."""
        streaming_client.conn = mock_duckdb_connection
        test_batch = [
            {
                "event_body": f"msg_{i}",
                "partition_id": str(i % 3),
                "sequence_number": 100 + i,
                "enqueued_time": "2025-10-17T12:00:00Z",
                "properties": None,
                "system_properties": None,
                "ingestion_timestamp": "2025-10-17T12:00:01Z",
            }
            for i in range(10)
        ]

        result = streaming_client.ingest_batch("test-channel", test_batch)

        assert result is True
        assert streaming_client.stats["total_messages_sent"] == 10
        assert streaming_client.stats["total_batches_sent"] == 1

    def test_ingest_batch_updates_last_ingestion(self, streaming_client, mock_duckdb_connection):
        """Test that ingest_batch updates last_ingestion timestamp."""
        streaming_client.conn = mock_duckdb_connection
        test_batch = [
            {
                "event_body": "test",
                "partition_id": "0",
                "sequence_number": 100,
                "enqueued_time": "2025-10-17T12:00:00Z",
                "properties": None,
                "system_properties": None,
                "ingestion_timestamp": "2025-10-17T12:00:01Z",
            }
        ]

        before_ingest = datetime.now(timezone.utc)
        streaming_client.ingest_batch("test-channel", test_batch)
        after_ingest = datetime.now(timezone.utc)

        assert streaming_client.stats["last_ingestion"] is not None
        assert before_ingest <= streaming_client.stats["last_ingestion"] <= after_ingest

    def test_ingest_batch_error_handling(self, streaming_client, mock_duckdb_connection):
        """Test error handling during batch ingestion."""
        streaming_client.conn = mock_duckdb_connection
        mock_duckdb_connection.execute.side_effect = Exception("Database error")
        test_batch = [{"event_body": "test", "partition_id": "0", "sequence_number": 1}]

        result = streaming_client.ingest_batch("test-channel", test_batch)

        assert result is False
        # Stats should not be updated on error
        assert streaming_client.stats["total_messages_sent"] == 0


# ============================================================================
# Channel Management Tests
# ============================================================================


class TestMotherDuckStreamingClientChannelManagement:
    """Test channel name creation."""

    def test_create_channel_name_default(self, streaming_client):
        """Test channel name creation with defaults."""
        channel_name = streaming_client.create_channel_name("my-eventhub")

        assert "my-eventhub" in channel_name
        assert "dev" in channel_name
        assert "default" in channel_name
        assert "test-client" in channel_name

    def test_create_channel_name_custom_env(self, streaming_client):
        """Test channel name creation with custom environment."""
        channel_name = streaming_client.create_channel_name("my-eventhub", environment="prod")

        assert "my-eventhub" in channel_name
        assert "prod" in channel_name

    def test_create_channel_name_custom_region(self, streaming_client):
        """Test channel name creation with custom region."""
        channel_name = streaming_client.create_channel_name("my-eventhub", region="us-east-1")

        assert "my-eventhub" in channel_name
        assert "us-east-1" in channel_name

    def test_create_channel_name_format(self, streaming_client):
        """Test channel name format is consistent."""
        channel_name = streaming_client.create_channel_name("hub", "prod", "us-west")

        # Format should be: eventhub-env-region-suffix
        parts = channel_name.split("-")
        assert len(parts) >= 4


# ============================================================================
# Statistics Tests
# ============================================================================


class TestMotherDuckStreamingClientStatistics:
    """Test statistics tracking."""

    def test_get_stats_no_activity(self, streaming_client):
        """Test getting stats without any activity."""
        stats = streaming_client.get_stats()

        assert stats["total_messages_sent"] == 0
        assert stats["total_batches_sent"] == 0
        assert "runtime_seconds" not in stats or stats.get("runtime_seconds", 0) == 0

    def test_get_stats_includes_runtime(self, streaming_client):
        """Test that stats include runtime calculation."""
        streaming_client.stats["client_created_at"] = datetime.now(timezone.utc)

        stats = streaming_client.get_stats()

        assert "runtime_seconds" in stats
        assert stats["runtime_seconds"] >= 0

    def test_get_stats_messages_per_second(self, streaming_client):
        """Test that stats calculate messages per second."""
        # Set client_created_at to 1 second in the past to ensure runtime > 0
        from datetime import timedelta
        streaming_client.stats["client_created_at"] = datetime.now(timezone.utc) - timedelta(seconds=1)
        streaming_client.stats["total_messages_sent"] = 100

        stats = streaming_client.get_stats()

        assert "messages_per_second" in stats
        assert stats["messages_per_second"] > 0

    def test_get_stats_copy_not_reference(self, streaming_client):
        """Test that get_stats returns a copy, not a reference."""
        stats1 = streaming_client.get_stats()
        stats1["total_messages_sent"] = 999

        stats2 = streaming_client.get_stats()

        assert stats2["total_messages_sent"] == 0


# ============================================================================
# Health Check Tests
# ============================================================================


class TestMotherDuckStreamingClientHealthCheck:
    """Test health check functionality."""

    def test_health_check_no_connection(self, streaming_client):
        """Test health check with no connection."""
        streaming_client.conn = None

        health = streaming_client.health_check()

        assert health["client_status"] == "stopped"
        assert health["connection_active"] is False
        assert len(health["errors"]) > 0

    def test_health_check_connection_active(self, streaming_client, mock_duckdb_connection):
        """Test health check with active connection."""
        streaming_client.conn = mock_duckdb_connection
        mock_duckdb_connection.execute.return_value.fetchone.return_value = (1,)

        health = streaming_client.health_check()

        assert health["client_status"] == "running"
        assert health["connection_active"] is True
        assert len(health["errors"]) == 0

    def test_health_check_connection_test_failed(self, streaming_client, mock_duckdb_connection):
        """Test health check when connection test fails."""
        streaming_client.conn = mock_duckdb_connection
        mock_duckdb_connection.execute.return_value.fetchone.return_value = None

        health = streaming_client.health_check()

        assert health["client_status"] != "running"
        assert len(health["errors"]) > 0

    def test_health_check_connection_exception(self, streaming_client, mock_duckdb_connection):
        """Test health check when connection raises exception."""
        streaming_client.conn = mock_duckdb_connection
        mock_duckdb_connection.execute.side_effect = Exception("Connection error")

        health = streaming_client.health_check()

        assert health["connection_active"] is False
        assert len(health["errors"]) > 0


# ============================================================================
# Configuration Tests
# ============================================================================


class TestMotherDuckStreamingClientConfiguration:
    """Test configuration management."""

    def test_client_preserves_database_config(self, sample_motherduck_config):
        """Test that client preserves database configuration."""
        client = MotherDuckStreamingClient(
            motherduck_config=sample_motherduck_config,
        )

        assert client.motherduck_config.database == "test_database"
        assert client.motherduck_config.schema_name == "test_schema"
        assert client.motherduck_config.table_name == "test_table"

    def test_client_with_connection_config(self, sample_motherduck_config):
        """Test client initialization with connection config."""
        from src.utils.motherduck import MotherDuckConnectionConfig

        conn_config = MotherDuckConnectionConfig(
            token="token123",
            target_db="db",
            target_schema="schema",
            target_table="table",
        )

        client = MotherDuckStreamingClient(
            motherduck_config=sample_motherduck_config,
            connection_config=conn_config,
        )

        assert client.connection_config == conn_config


# ============================================================================
# Integration Tests
# ============================================================================


class TestMotherDuckStreamingClientIntegration:
    """Test integration scenarios."""

    @patch("src.streaming.motherduck.duckdb.connect")
    def test_full_lifecycle(self, mock_duckdb_connect, streaming_client, mock_duckdb_connection):
        """Test complete client lifecycle: start, ingest, stop."""
        mock_duckdb_connect.return_value = mock_duckdb_connection

        # Start
        streaming_client.start()
        assert streaming_client.conn is not None

        # Ingest batch
        test_batch = [
            {
                "event_body": "test",
                "partition_id": "0",
                "sequence_number": 100,
                "enqueued_time": "2025-10-17T12:00:00Z",
                "properties": None,
                "system_properties": None,
                "ingestion_timestamp": "2025-10-17T12:00:01Z",
            }
        ]
        result = streaming_client.ingest_batch("test-channel", test_batch)
        assert result is True
        assert streaming_client.stats["total_messages_sent"] == 1

        # Stop
        streaming_client.stop()
        assert streaming_client.conn is None

    def test_multiple_batches_accumulate_stats(self, streaming_client, mock_duckdb_connection):
        """Test that multiple batch ingestions accumulate statistics."""
        streaming_client.conn = mock_duckdb_connection

        for batch_num in range(5):
            batch = [
                {
                    "event_body": f"msg_{i}",
                    "partition_id": "0",
                    "sequence_number": 100 + i,
                    "enqueued_time": "2025-10-17T12:00:00Z",
                    "properties": None,
                    "system_properties": None,
                    "ingestion_timestamp": "2025-10-17T12:00:01Z",
                }
                for i in range(10)
            ]
            streaming_client.ingest_batch(f"channel-{batch_num}", batch)

        assert streaming_client.stats["total_messages_sent"] == 50
        assert streaming_client.stats["total_batches_sent"] == 5
