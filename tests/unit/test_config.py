"""
Unit tests for configuration module (src/utils/config.py).

Tests cover:
- EventHubConfig validation (namespace, name, consumer_group)
- MotherDuckConfig validation
- EventHubMotherDuckMapping validation
- Environment variable parsing
- Error handling for invalid configurations
"""

import os
from typing import Any, Dict

import pytest
from pydantic import ValidationError

from src.utils.config import (
    EventHubConfig,
    EventHubMotherDuckMapping,
    MotherDuckConfig,
)


# ============================================================================
# EventHubConfig Tests
# ============================================================================


class TestEventHubConfigValidation:
    """Test EventHubConfig validation and field requirements."""

    def test_valid_eventhub_config(self, sample_eventhub_config: EventHubConfig):
        """Test creating a valid EventHub configuration."""
        assert sample_eventhub_config.name == "topic1"
        assert sample_eventhub_config.namespace == "eventhu1.servicebus.windows.net"
        assert sample_eventhub_config.consumer_group == "$Default"
        assert sample_eventhub_config.max_batch_size == 1000

    def test_eventhub_namespace_validation_valid_formats(self):
        """Test namespace validation with valid formats."""
        valid_namespaces = [
            "myeventhub.servicebus.windows.net",
            "prod-eh.servicebus.windows.net",
            "test123.servicebus.windows.net",
        ]
        for namespace in valid_namespaces:
            config = EventHubConfig(
                name="test",
                namespace=namespace,
                consumer_group="$Default",
            )
            assert config.namespace == namespace

    def test_eventhub_namespace_validation_invalid_formats(self):
        """Test namespace validation rejects invalid formats."""
        invalid_namespaces = [
            "myeventhub.windows.net",
            "myeventhub.servicebus.com",
            "myeventhub",
        ]
        for namespace in invalid_namespaces:
            with pytest.raises(ValidationError) as exc_info:
                EventHubConfig(
                    name="test",
                    namespace=namespace,
                    consumer_group="$Default",
                )
            assert "namespace" in str(exc_info.value).lower()

    @pytest.mark.parametrize(
        "valid_name",
        [
            "topic1",
            "my-topic",
            "my_topic",
            "topic.1",
            "MyTopic123",
        ],
    )
    def test_eventhub_name_validation_valid_formats(self, valid_name: str):
        """Test name validation with valid formats."""
        config = EventHubConfig(
            name=valid_name,
            namespace="test.servicebus.windows.net",
            consumer_group="$Default",
        )
        assert config.name == valid_name

    @pytest.mark.parametrize(
        "invalid_name",
        [
            "-topic",  # starts with dash
            "topic-",  # ends with dash
            "_topic",  # starts with underscore
            "topic_",  # ends with underscore
            ".topic",  # starts with dot
            "topic.",  # ends with dot
            "topic@name",  # invalid char
        ],
    )
    def test_eventhub_name_validation_invalid_formats(self, invalid_name: str):
        """Test name validation rejects invalid formats."""
        with pytest.raises(ValidationError):
            EventHubConfig(
                name=invalid_name,
                namespace="test.servicebus.windows.net",
                consumer_group="$Default",
            )

    def test_eventhub_consumer_group_cannot_be_empty(self):
        """Test consumer group cannot be empty."""
        with pytest.raises(ValidationError):
            EventHubConfig(
                name="test",
                namespace="test.servicebus.windows.net",
                consumer_group="",
            )

    def test_eventhub_consumer_group_cannot_be_whitespace(self):
        """Test consumer group cannot be whitespace only."""
        with pytest.raises(ValidationError):
            EventHubConfig(
                name="test",
                namespace="test.servicebus.windows.net",
                consumer_group="   ",
            )

    def test_eventhub_config_defaults(self):
        """Test EventHub config has correct defaults."""
        config = EventHubConfig(
            name="test",
            namespace="test.servicebus.windows.net",
            consumer_group="$Default",
        )
        assert config.max_batch_size == 1000
        assert config.max_wait_time == 60
        assert config.prefetch_count == 300
        assert config.checkpoint_interval_seconds == 300
        assert config.max_message_batch_size == 1000
        assert config.batch_timeout_seconds == 300
        assert config.use_connection_string is False

    def test_eventhub_config_custom_values(self):
        """Test EventHub config accepts custom values."""
        config = EventHubConfig(
            name="test",
            namespace="test.servicebus.windows.net",
            consumer_group="$Default",
            max_batch_size=500,
            checkpoint_interval_seconds=60,
            batch_timeout_seconds=120,
        )
        assert config.max_batch_size == 500
        assert config.checkpoint_interval_seconds == 60
        assert config.batch_timeout_seconds == 120


# ============================================================================
# MotherDuckConfig Tests
# ============================================================================


class TestMotherDuckConfigValidation:
    """Test MotherDuckConfig validation and field requirements."""

    def test_valid_motherduck_config(self, sample_motherduck_config: MotherDuckConfig):
        """Test creating a valid MotherDuck configuration."""
        assert sample_motherduck_config.database == "mother_ducklake"
        assert sample_motherduck_config.schema_name == "ingest"
        assert sample_motherduck_config.table_name == "table1"
        assert sample_motherduck_config.token == "test_token_12345"

    def test_motherduck_config_required_fields(self):
        """Test that all required fields are enforced."""
        required_fields = ["database", "schema_name", "table_name", "token"]
        base_config = {
            "database": "test_db",
            "schema_name": "test_schema",
            "table_name": "test_table",
            "token": "test_token",
        }

        for field in required_fields:
            incomplete_config = {k: v for k, v in base_config.items() if k != field}
            with pytest.raises(ValidationError):
                MotherDuckConfig(**incomplete_config)

    def test_motherduck_config_defaults(self):
        """Test MotherDuck config has correct defaults."""
        config = MotherDuckConfig(
            database="test_db",
            schema_name="test_schema",
            table_name="test_table",
            token="test_token",
        )
        assert config.batch_size == 1000
        assert config.max_retry_attempts == 3
        assert config.retry_delay_seconds == 5
        assert config.connection_timeout_seconds == 30

    def test_motherduck_config_custom_values(self):
        """Test MotherDuck config accepts custom values."""
        config = MotherDuckConfig(
            database="test_db",
            schema_name="test_schema",
            table_name="test_table",
            token="test_token",
            batch_size=500,
            max_retry_attempts=5,
            retry_delay_seconds=10,
        )
        assert config.batch_size == 500
        assert config.max_retry_attempts == 5
        assert config.retry_delay_seconds == 10


# ============================================================================
# EventHubMotherDuckMapping Tests
# ============================================================================


class TestEventHubMotherDuckMappingValidation:
    """Test EventHubMotherDuckMapping validation."""

    def test_valid_mapping(self, sample_mapping: EventHubMotherDuckMapping):
        """Test creating a valid mapping."""
        assert sample_mapping.event_hub_key == "EVENTHUBNAME_1"
        assert sample_mapping.motherduck_key == "MOTHERDUCK_1"

    def test_mapping_keys_must_be_uppercase(self):
        """Test mapping keys must be uppercase with underscores."""
        with pytest.raises(ValidationError):
            EventHubMotherDuckMapping(
                event_hub_key="eventhubname_1",  # lowercase invalid
                motherduck_key="MOTHERDUCK_1",
            )

        with pytest.raises(ValidationError):
            EventHubMotherDuckMapping(
                event_hub_key="EVENTHUBNAME-1",  # dash invalid
                motherduck_key="MOTHERDUCK_1",
            )

    def test_mapping_required_fields(self):
        """Test that mapping requires both keys."""
        with pytest.raises(ValidationError):
            EventHubMotherDuckMapping(event_hub_key="EVENTHUBNAME_1")

        with pytest.raises(ValidationError):
            EventHubMotherDuckMapping(motherduck_key="MOTHERDUCK_1")
