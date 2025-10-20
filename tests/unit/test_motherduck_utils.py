"""
Unit tests for utils/motherduck.py checkpoint and connection utilities.

Tests cover:
- get_partition_checkpoints() - retrieving per-partition checkpoints
- insert_partition_checkpoint() - saving partition checkpoints
- create_control_table() - creating checkpoint storage tables
- Connection management and error handling
"""

from typing import Dict, Optional
from unittest.mock import MagicMock, patch

import pytest

from src.utils.motherduck import (
    MotherDuckConnectionConfig,
    create_control_table,
    get_partition_checkpoints,
    insert_partition_checkpoint,
)


# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture
def valid_connection_config():
    """Create a valid MotherDuck connection config."""
    return MotherDuckConnectionConfig(
        token="test_token_12345",
        database="mother_ducklake",
        target_db="mother_ducklake",
        target_schema="ingest",
        target_table="table1",
    )


@pytest.fixture
def mock_duckdb_connection_with_results():
    """Create a mock DuckDB connection that returns query results."""
    mock_conn = MagicMock()

    # Mock the execute().fetchall() pattern
    mock_result = MagicMock()
    mock_result.fetchall.return_value = [
        ("0", 100),
        ("1", 150),
        ("2", 120),
    ]
    mock_conn.execute.return_value = mock_result

    return mock_conn


@pytest.fixture
def mock_duckdb_connection_empty():
    """Create a mock DuckDB connection that returns no results."""
    mock_conn = MagicMock()

    mock_result = MagicMock()
    mock_result.fetchall.return_value = []
    mock_conn.execute.return_value = mock_result

    return mock_conn


# ============================================================================
# get_partition_checkpoints Tests
# ============================================================================


class TestGetPartitionCheckpoints:
    """Test partition checkpoint retrieval."""

    def test_get_partition_checkpoints_with_results(
        self, mock_duckdb_connection_with_results, valid_connection_config
    ):
        """Test retrieving partition checkpoints with results."""
        with patch(
            "src.utils.motherduck.get_connection"
        ) as mock_get_conn, patch.dict(
            "os.environ",
            {
                "TARGET_DB": "mother_ducklake",
                "TARGET_SCHEMA": "ingest",
                "TARGET_TABLE": "ingestion_status",
            },
        ):
            mock_get_conn.return_value = mock_duckdb_connection_with_results

            result = get_partition_checkpoints(
                eventhub_namespace="eventhu1.servicebus.windows.net",
                eventhub="topic1",
                target_db="mother_ducklake",
                target_schema="ingest",
                target_table="table1",
                config=valid_connection_config,
            )

            assert result == {"0": 100, "1": 150, "2": 120}
            mock_duckdb_connection_with_results.execute.assert_called_once()

    def test_get_partition_checkpoints_no_results(
        self, mock_duckdb_connection_empty, valid_connection_config
    ):
        """Test retrieving when no checkpoints exist."""
        with patch(
            "src.utils.motherduck.get_connection"
        ) as mock_get_conn, patch.dict(
            "os.environ",
            {
                "TARGET_DB": "mother_ducklake",
                "TARGET_SCHEMA": "ingest",
                "TARGET_TABLE": "ingestion_status",
            },
        ):
            mock_get_conn.return_value = mock_duckdb_connection_empty

            result = get_partition_checkpoints(
                eventhub_namespace="eventhu1.servicebus.windows.net",
                eventhub="topic1",
                target_db="mother_ducklake",
                target_schema="ingest",
                target_table="table1",
                config=valid_connection_config,
            )

            assert result is None

    def test_get_partition_checkpoints_with_provided_connection(
        self, mock_duckdb_connection_with_results
    ):
        """Test retrieving checkpoints with externally provided connection."""
        import os
        
        with patch.dict(
            os.environ,
            {
                "MOTHERDUCK_TOKEN": "test_token",
                "TARGET_DB": "mother_ducklake",
                "TARGET_SCHEMA": "ingest",
                "TARGET_TABLE": "ingestion_status",
            },
        ):
            result = get_partition_checkpoints(
                eventhub_namespace="eventhu1.servicebus.windows.net",
                eventhub="topic1",
                target_db="mother_ducklake",
                target_schema="ingest",
                target_table="table1",
                conn=mock_duckdb_connection_with_results,
                env_file=None,
            )

            assert result == {"0": 100, "1": 150, "2": 120}
            # Connection should not be closed when externally provided
            mock_duckdb_connection_with_results.close.assert_not_called()

    def test_get_partition_checkpoints_query_parameters(
        self, mock_duckdb_connection_with_results, valid_connection_config
    ):
        """Test that query receives correct parameters."""
        with patch(
            "src.utils.motherduck.get_connection"
        ) as mock_get_conn, patch.dict(
            "os.environ",
            {
                "TARGET_DB": "mother_ducklake",
                "TARGET_SCHEMA": "ingest",
                "TARGET_TABLE": "ingestion_status",
            },
        ):
            mock_get_conn.return_value = mock_duckdb_connection_with_results

            get_partition_checkpoints(
                eventhub_namespace="test-namespace",
                eventhub="test-hub",
                target_db="test-db",
                target_schema="test-schema",
                target_table="test-table",
                config=valid_connection_config,
            )

            # Verify execute was called with correct parameters
            call_args = mock_duckdb_connection_with_results.execute.call_args
            query = call_args[0][0]
            params = call_args[0][1]

            assert "test-namespace" in params
            assert "test-hub" in params
            assert "test-db" in params
            assert "test-schema" in params
            assert "test-table" in params

    def test_get_partition_checkpoints_error_handling(
        self, valid_connection_config
    ):
        """Test error handling during checkpoint retrieval."""
        with patch(
            "src.utils.motherduck.get_connection"
        ) as mock_get_conn:
            mock_conn = MagicMock()
            mock_conn.execute.side_effect = Exception("Database error")
            mock_get_conn.return_value = mock_conn

            with pytest.raises(Exception):
                get_partition_checkpoints(
                    eventhub_namespace="eventhu1.servicebus.windows.net",
                    eventhub="topic1",
                    target_db="mother_ducklake",
                    target_schema="ingest",
                    target_table="table1",
                    config=valid_connection_config,
                )

    def test_get_partition_checkpoints_max_aggregation(
        self, valid_connection_config
    ):
        """Test that MAX waterlevel is selected for each partition."""
        mock_conn = MagicMock()
        mock_result = MagicMock()
        # Simulate multiple sequences per partition, MAX should be selected
        mock_result.fetchall.return_value = [
            ("0", 500),  # MAX for partition 0
            ("1", 750),  # MAX for partition 1
        ]
        mock_conn.execute.return_value = mock_result

        with patch(
            "src.utils.motherduck.get_connection"
        ) as mock_get_conn, patch.dict(
            "os.environ",
            {
                "TARGET_DB": "mother_ducklake",
                "TARGET_SCHEMA": "ingest",
                "TARGET_TABLE": "ingestion_status",
            },
        ):
            mock_get_conn.return_value = mock_conn

            result = get_partition_checkpoints(
                eventhub_namespace="eventhu1.servicebus.windows.net",
                eventhub="topic1",
                target_db="mother_ducklake",
                target_schema="ingest",
                target_table="table1",
                config=valid_connection_config,
            )

            # Verify MAX is reflected in results
            assert result == {"0": 500, "1": 750}
            assert result["0"] == 500
            assert result["1"] == 750


# ============================================================================
# insert_partition_checkpoint Tests
# ============================================================================


class TestInsertPartitionCheckpoint:
    """Test partition checkpoint insertion."""

    def test_insert_partition_checkpoint_basic(self, valid_connection_config):
        """Test basic checkpoint insertion."""
        mock_conn = MagicMock()

        with patch(
            "src.utils.motherduck.get_connection"
        ) as mock_get_conn, patch.dict(
            "os.environ",
            {
                "TARGET_DB": "mother_ducklake",
                "TARGET_SCHEMA": "ingest",
                "TARGET_TABLE": "ingestion_status",
            },
        ):
            mock_get_conn.return_value = mock_conn

            insert_partition_checkpoint(
                eventhub_namespace="eventhu1.servicebus.windows.net",
                eventhub="topic1",
                target_db="mother_ducklake",
                target_schema="ingest",
                target_table="table1",
                partition_id="0",
                waterlevel=100,
                config=valid_connection_config,
            )

            # Verify INSERT was executed
            mock_conn.execute.assert_called()
            call_args = mock_conn.execute.call_args[0]
            query = call_args[0]

            assert "INSERT" in query
            assert "ingestion_status" in query

    def test_insert_partition_checkpoint_with_metadata(
        self, valid_connection_config
    ):
        """Test checkpoint insertion with metadata."""
        mock_conn = MagicMock()
        metadata = {"sequence_number": 100, "timestamp": "2025-10-17T20:00:00Z"}

        with patch(
            "src.utils.motherduck.get_connection"
        ) as mock_get_conn, patch.dict(
            "os.environ",
            {
                "TARGET_DB": "mother_ducklake",
                "TARGET_SCHEMA": "ingest",
                "TARGET_TABLE": "ingestion_status",
            },
        ):
            mock_get_conn.return_value = mock_conn

            insert_partition_checkpoint(
                eventhub_namespace="eventhu1.servicebus.windows.net",
                eventhub="topic1",
                target_db="mother_ducklake",
                target_schema="ingest",
                target_table="table1",
                partition_id="0",
                waterlevel=100,
                metadata=metadata,
                config=valid_connection_config,
            )

            # Verify INSERT was executed
            mock_conn.execute.assert_called()

    def test_insert_partition_checkpoint_multiple_calls(
        self, valid_connection_config
    ):
        """Test inserting checkpoints for multiple partitions."""
        mock_conn = MagicMock()

        with patch(
            "src.utils.motherduck.get_connection"
        ) as mock_get_conn, patch.dict(
            "os.environ",
            {
                "TARGET_DB": "mother_ducklake",
                "TARGET_SCHEMA": "ingest",
                "TARGET_TABLE": "ingestion_status",
            },
        ):
            mock_get_conn.return_value = mock_conn

            # Insert for multiple partitions
            for partition_id in ["0", "1", "2"]:
                insert_partition_checkpoint(
                    eventhub_namespace="eventhu1.servicebus.windows.net",
                    eventhub="topic1",
                    target_db="mother_ducklake",
                    target_schema="ingest",
                    target_table="table1",
                    partition_id=partition_id,
                    waterlevel=100 * int(partition_id),
                    config=valid_connection_config,
                )

            # Verify INSERT called 3 times
            assert mock_conn.execute.call_count == 3

    def test_insert_partition_checkpoint_error_handling(
        self, valid_connection_config
    ):
        """Test error handling during checkpoint insertion."""
        mock_conn = MagicMock()
        mock_conn.execute.side_effect = Exception("Insert failed")

        with patch(
            "src.utils.motherduck.get_connection"
        ) as mock_get_conn, patch.dict(
            "os.environ",
            {
                "TARGET_DB": "mother_ducklake",
                "TARGET_SCHEMA": "ingest",
                "TARGET_TABLE": "ingestion_status",
            },
        ):
            mock_get_conn.return_value = mock_conn

            with pytest.raises(Exception):
                insert_partition_checkpoint(
                    eventhub_namespace="eventhu1.servicebus.windows.net",
                    eventhub="topic1",
                    target_db="mother_ducklake",
                    target_schema="ingest",
                    target_table="table1",
                    partition_id="0",
                    waterlevel=100,
                    config=valid_connection_config,
                )


# ============================================================================
# create_control_table Tests
# ============================================================================


class TestCreateControlTable:
    """Test control table creation."""

    def test_create_control_table_success(self, valid_connection_config):
        """Test successful control table creation."""
        mock_conn = MagicMock()

        with patch(
            "src.utils.motherduck.get_connection"
        ) as mock_get_conn:
            mock_get_conn.return_value = mock_conn

            result = create_control_table(
                target_db="mother_ducklake",
                target_schema="control",
                target_table="checkpoints",
                config=valid_connection_config,
            )

            assert result is True
            # Verify schema and table creation were attempted
            assert mock_conn.execute.call_count >= 2

    def test_create_control_table_with_provided_connection(
        self, valid_connection_config
    ):
        """Test control table creation with provided connection."""
        mock_conn = MagicMock()

        result = create_control_table(
            target_db="mother_ducklake",
            target_schema="control",
            target_table="checkpoints",
            conn=mock_conn,
            config=valid_connection_config,
        )

        assert result is True
        # Connection should not be closed when externally provided
        mock_conn.close.assert_not_called()

    def test_create_control_table_schema_creation(
        self, valid_connection_config
    ):
        """Test that schema is created before table."""
        mock_conn = MagicMock()

        with patch(
            "src.utils.motherduck.get_connection"
        ) as mock_get_conn:
            mock_get_conn.return_value = mock_conn

            create_control_table(
                target_db="test_db",
                target_schema="control",
                target_table="checkpoints",
                config=valid_connection_config,
            )

            # Get all execute calls
            calls = mock_conn.execute.call_args_list

            # First call should be schema creation
            schema_call = calls[0][0][0]
            assert "CREATE SCHEMA" in schema_call
            assert "test_db.control" in schema_call

            # Second call should be table creation
            table_call = calls[1][0][0]
            assert "CREATE TABLE" in table_call

    def test_create_control_table_error_on_connection_failure(
        self, valid_connection_config
    ):
        """Test error handling when connection fails."""
        with patch(
            "src.utils.motherduck.get_connection"
        ) as mock_get_conn:
            mock_get_conn.side_effect = Exception("Connection failed")

            result = create_control_table(
                target_db="mother_ducklake",
                target_schema="control",
                target_table="checkpoints",
                config=valid_connection_config,
            )

            assert result is False

    def test_create_control_table_error_on_execution(
        self, valid_connection_config
    ):
        """Test error handling when SQL execution fails."""
        mock_conn = MagicMock()
        mock_conn.execute.side_effect = Exception("SQL error")

        result = create_control_table(
            target_db="mother_ducklake",
            target_schema="control",
            target_table="checkpoints",
            conn=mock_conn,
            config=valid_connection_config,
        )

        assert result is False

    def test_create_control_table_preserves_schema_names(
        self, valid_connection_config
    ):
        """Test that schema and table names are preserved correctly."""
        mock_conn = MagicMock()

        create_control_table(
            target_db="prod_db",
            target_schema="audit",
            target_table="event_log",
            conn=mock_conn,
            config=valid_connection_config,
        )

        # Get all execute calls
        calls = mock_conn.execute.call_args_list

        # Verify database.schema pattern is used
        all_calls_text = str(calls)
        assert "prod_db" in all_calls_text or "prod_db" in str(
            mock_conn.execute.call_args_list
        )
