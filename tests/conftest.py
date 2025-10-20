"""
Pytest configuration and shared fixtures for StreamDuck tests.

This module provides reusable fixtures for mocking external dependencies
and creating test data consistently across all test modules.
"""

import asyncio
import json
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest

from src.consumers.eventhub import EventHubMessage
from src.utils.config import (
    EventHubConfig,
    EventHubMotherDuckMapping,
    MotherDuckConfig,
    StreamDuckConfig,
)


# ============================================================================
# Fixtures: Configuration Objects
# ============================================================================


@pytest.fixture
def sample_eventhub_config() -> EventHubConfig:
    """Create a sample EventHub configuration for testing."""
    return EventHubConfig(
        name="topic1",
        namespace="eventhu1.servicebus.windows.net",
        consumer_group="$Default",
        max_batch_size=1000,
        max_wait_time=60,
        prefetch_count=300,
        checkpoint_interval_seconds=300,
        max_message_batch_size=1000,
        batch_timeout_seconds=300,
    )


@pytest.fixture
def sample_motherduck_config() -> MotherDuckConfig:
    """Create a sample MotherDuck configuration for testing."""
    return MotherDuckConfig(
        database="mother_ducklake",
        schema_name="ingest",
        table_name="table1",
        token="test_token_12345",
        batch_size=1000,
        max_retry_attempts=3,
        retry_delay_seconds=5,
        connection_timeout_seconds=30,
    )


@pytest.fixture
def sample_mapping() -> EventHubMotherDuckMapping:
    """Create a sample EventHub to MotherDuck mapping."""
    return EventHubMotherDuckMapping(
        event_hub_key="EVENTHUBNAME_1",
        motherduck_key="MOTHERDUCK_1",
    )


# ============================================================================
# Fixtures: Message Objects
# ============================================================================


def _create_mock_event_data(body_dict: Dict[str, Any], enqueued_time=None) -> MagicMock:
    """Create a mock EventData object for testing."""
    if enqueued_time is None:
        enqueued_time = datetime.now(timezone.utc)
    
    mock_event = MagicMock()
    mock_event.body_as_str.return_value = json.dumps(body_dict)
    mock_event.enqueued_time = enqueued_time
    mock_event.properties = {"key": "value"}
    mock_event.system_properties = {"offset": "0", "sequence_number": "100"}
    return mock_event


@pytest.fixture
def sample_eventhub_message() -> EventHubMessage:
    """Create a sample EventHub message for testing."""
    mock_event_data = _create_mock_event_data({
        "source_system": "topic_1",
        "timestamp": "2025-10-17T20:00:00Z",
        "message_id": "msg-123",
        "payload": {"value": 42},
    })
    return EventHubMessage(
        event_data=mock_event_data,
        partition_id="0",
        sequence_number=100,
    )


@pytest.fixture
def sample_eventhub_messages() -> List[EventHubMessage]:
    """Create multiple sample EventHub messages for batch testing."""
    messages = []
    for i in range(10):
        mock_event_data = _create_mock_event_data({
            "source_system": "topic_1",
            "message_id": f"msg-{i}",
            "value": i,
        })
        msg = EventHubMessage(
            event_data=mock_event_data,
            partition_id=str(i % 3),  # Distribute across 3 partitions
            sequence_number=100 + i,
        )
        messages.append(msg)
    return messages


# ============================================================================
# Fixtures: Mock Azure SDK Components
# ============================================================================


@pytest.fixture
def mock_eventhub_consumer_client() -> AsyncMock:
    """Create a mock EventHubConsumerClient for testing."""
    mock_client = AsyncMock()
    mock_client.get_partition_ids = AsyncMock(return_value=["0", "1", "2"])
    mock_client.receive_batch = AsyncMock()
    mock_client.close = AsyncMock()
    return mock_client


@pytest.fixture
def mock_azure_credential() -> MagicMock:
    """Create a mock Azure credential for testing."""
    mock_cred = MagicMock()
    mock_cred.get_token = MagicMock(
        return_value=MagicMock(token="test_token", expires_on=time.time() + 3600)
    )
    return mock_cred


@pytest.fixture
def mock_azure_async_credential() -> AsyncMock:
    """Create a mock async Azure credential for testing."""
    mock_cred = AsyncMock()
    mock_cred.get_token = AsyncMock(
        return_value=MagicMock(token="test_token", expires_on=time.time() + 3600)
    )
    mock_cred.__aenter__ = AsyncMock(return_value=mock_cred)
    mock_cred.__aexit__ = AsyncMock(return_value=None)
    return mock_cred


# ============================================================================
# Fixtures: Mock MotherDuck/DuckDB Components
# ============================================================================


@pytest.fixture
def mock_duckdb_connection() -> MagicMock:
    """Create a mock DuckDB connection for testing."""
    mock_conn = MagicMock()
    mock_conn.sql = MagicMock(return_value=MagicMock(fetchall=MagicMock(return_value=[])))
    mock_conn.execute = MagicMock(return_value=MagicMock(fetchall=MagicMock(return_value=[])))
    return mock_conn


@pytest.fixture
def mock_motherduck_session() -> MagicMock:
    """Create a mock MotherDuck session for testing."""
    mock_session = MagicMock()
    mock_session.sql = MagicMock(return_value=MagicMock(fetchall=MagicMock(return_value=[])))
    mock_session.execute = MagicMock()
    return mock_session


# ============================================================================
# Fixtures: Async Utilities
# ============================================================================


@pytest.fixture
def event_loop():
    """Create an event loop for async tests."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    yield loop
    loop.close()


@pytest.fixture
async def async_sample_eventhub_message() -> EventHubMessage:
    """Create a sample EventHub message for async testing."""
    return EventHubMessage(
        partition_id="0",
        sequence_number=100,
        enqueued_time="2025-10-17T20:00:00Z",
        event_body={
            "source_system": "topic_1",
            "timestamp": "2025-10-17T20:00:00Z",
            "message_id": "msg-123",
            "payload": {"value": 42},
        },
    )


# ============================================================================
# Fixtures: Mock Utilities
# ============================================================================


@pytest.fixture
def mock_time(monkeypatch):
    """Mock time.time() for testing time-based logic."""
    class MockTime:
        def __init__(self):
            self.current_time = 1000.0

        def __call__(self):
            return self.current_time

        def advance(self, seconds: float):
            self.current_time += seconds

        def reset(self):
            self.current_time = 1000.0

    mock_time_obj = MockTime()
    monkeypatch.setattr("time.time", mock_time_obj)
    return mock_time_obj


# ============================================================================
# Fixtures: Environment Variables
# ============================================================================


@pytest.fixture
def env_setup(monkeypatch):
    """Set up environment variables for testing."""
    test_env = {
        "EVENTHUBNAME_1_NAMESPACE": "eventhu1.servicebus.windows.net",
        "EVENTHUBNAME_1_NAME": "topic1",
        "EVENTHUBNAME_1_CONSUMER_GROUP": "$Default",
        "MOTHERDUCK_1_DATABASE": "mother_ducklake",
        "MOTHERDUCK_1_SCHEMA_NAME": "ingest",
        "MOTHERDUCK_1_TABLE_NAME": "table1",
        "MOTHERDUCK_1_TOKEN": "test_token_12345",
    }
    for key, value in test_env.items():
        monkeypatch.setenv(key, value)
    return test_env


# ============================================================================
# Pytest Configuration
# ============================================================================


def pytest_configure(config):
    """Configure pytest with custom markers."""
    config.addinivalue_line("markers", "asyncio: mark test as an async test")
    config.addinivalue_line("markers", "unit: mark test as a unit test")
    config.addinivalue_line("markers", "integration: mark test as an integration test")
    config.addinivalue_line("markers", "slow: mark test as slow running")
