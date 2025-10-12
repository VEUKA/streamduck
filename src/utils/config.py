"""
Configuration module for StreamDuck pipeline.

This module provides configuration management for Azure Event Hubs and MotherDuck
integration, incorporating best practices for both services. It uses Pydantic Settings
for validation and python-dotenv for environment variable loading.

Best Practices Incorporated:
- Azure Event Hubs: Connection string management, client configuration
- MotherDuck: High-performance batch ingestion, connection management, metadata tracking
"""

import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, field_validator, model_validator
from pydantic_settings import BaseSettings


class EventHubConfig(BaseModel):
    """Configuration for a single Event Hub."""

    name: str = Field(..., description="Event Hub name")
    namespace: str = Field(..., description="Event Hub namespace")
    connection_string: Optional[str] = None
    consumer_group: str = Field(..., description="Consumer group name (required)")

    # Azure Event Hubs SDK best practices
    max_batch_size: int = 1000
    max_wait_time: int = 60
    prefetch_count: int = 300

    # Authentication settings
    use_connection_string: bool = Field(
        default=False,
        description="Use connection string instead of DefaultAzureCredential",
    )

    # Performance tuning
    checkpoint_interval_seconds: int = Field(
        default=300, description="How often to save checkpoints (seconds)"
    )
    max_message_batch_size: int = Field(
        default=1000, description="Maximum messages per batch for processing"
    )
    batch_timeout_seconds: int = Field(
        default=300, description="Maximum time to wait for batch completion"
    )

    @field_validator("namespace")
    @classmethod
    def validate_namespace(cls, v: str) -> str:
        """Validate Event Hub namespace format."""
        if not v.endswith(".servicebus.windows.net"):
            raise ValueError(
                "Event Hub namespace must end with .servicebus.windows.net"
            )
        return v

    @field_validator("name")
    @classmethod
    def validate_name(cls, v: str) -> str:
        """Validate Event Hub name format."""
        if not re.match(r"^[a-zA-Z0-9]([a-zA-Z0-9\-._])*[a-zA-Z0-9]$", v):
            raise ValueError("Event Hub name contains invalid characters")
        return v

    @field_validator("consumer_group")
    @classmethod
    def validate_consumer_group(cls, v: str) -> str:
        """Validate consumer group format."""
        if not v.strip():
            raise ValueError("Consumer group cannot be empty")
        return v


class MotherDuckConfig(BaseModel):
    """Configuration for a single MotherDuck destination."""

    database: str = Field(..., description="MotherDuck database name")
    schema_name: str = Field(..., description="MotherDuck schema name")
    table_name: str = Field(..., description="MotherDuck table name")
    token: str = Field(..., description="MotherDuck authentication token")

    # MotherDuck batch ingestion best practices
    batch_size: int = Field(
        default=1000, description="Number of records per batch insert"
    )

    # Performance tuning
    max_retry_attempts: int = Field(
        default=3, description="Maximum retry attempts for failed operations"
    )
    retry_delay_seconds: int = Field(
        default=5, description="Delay between retry attempts"
    )
    connection_timeout_seconds: int = Field(
        default=30, description="Connection timeout in seconds"
    )

    @field_validator("database", "schema_name", "table_name")
    @classmethod
    def validate_motherduck_identifiers(cls, v: str) -> str:
        """Validate MotherDuck/DuckDB identifiers."""
        if not v.strip():
            raise ValueError(f"MotherDuck identifier cannot be empty: {v}")
        # DuckDB identifiers are more permissive than Snowflake
        return v.strip()

    @field_validator("token")
    @classmethod
    def validate_token(cls, v: str) -> str:
        """Validate MotherDuck token."""
        if not v.strip():
            raise ValueError("MotherDuck token cannot be empty")
        return v.strip()


class EventHubMotherDuckMapping(BaseModel):
    """Mapping between Event Hub and MotherDuck configurations."""

    event_hub_key: str = Field(..., description="Event Hub configuration key")
    motherduck_key: str = Field(..., description="MotherDuck configuration key")

    # Channel naming for tracking
    channel_name_pattern: str = "{event_hub}-{env}-{region}-{client_id}"

    @field_validator("event_hub_key", "motherduck_key")
    @classmethod
    def validate_mapping_keys(cls, v: str) -> str:
        """Validate mapping keys format."""
        if not re.match(r"^[A-Z0-9_]+$", v):
            raise ValueError(f"Mapping key must be uppercase with underscores: {v}")
        return v


class StreamDuckConfig(BaseSettings):
    """
    Main configuration class for StreamDuck pipeline.

    This class manages the complete configuration including Event Hubs, MotherDuck destinations,
    and their mappings. It incorporates best practices from both Azure Event Hubs and
    MotherDuck/DuckDB documentation.

    Environment Variables:
    - EVENTHUB_NAMESPACE: Event Hub namespace
    - EVENTHUBNAME_{N}: Event Hub names (where N is a number)
    - MOTHERDUCK_{N}_DATABASE: MotherDuck database name
    - MOTHERDUCK_{N}_SCHEMA: MotherDuck schema name
    - MOTHERDUCK_{N}_TABLE: MotherDuck table name
    - MOTHERDUCK_{N}_TOKEN: MotherDuck authentication token
    - EVENTHUBNAME_{N} = MOTHERDUCK_{M}: Mapping configuration
    """

    model_config = {
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        "case_sensitive": True,
        "extra": "ignore",
    }

    # Core Event Hub settings
    eventhub_namespace: str = Field(..., description="Event Hub namespace")

    # Environment and deployment settings
    environment: str = Field("development", description="Deployment environment")
    region: str = Field("default", description="Deployment region")

    # Performance settings
    max_concurrent_channels: int = Field(50, description="Maximum concurrent channels")
    ingestion_timeout_seconds: int = Field(
        300, description="Ingestion timeout in seconds"
    )

    # Pipeline performance tuning
    max_concurrent_mappings: int = Field(
        default=10, description="Maximum concurrent mapping processors"
    )
    health_check_interval_seconds: int = Field(
        default=60, description="Health check interval in seconds"
    )

    # Error handling and retry policies
    max_pipeline_restart_attempts: int = Field(
        default=3, description="Maximum pipeline restart attempts on failure"
    )
    pipeline_restart_delay_seconds: int = Field(
        default=30, description="Delay between pipeline restart attempts"
    )

    # Monitoring and logging
    enable_detailed_logging: bool = Field(
        default=False, description="Enable detailed debug logging"
    )
    log_message_samples: bool = Field(
        default=False, description="Log sample messages for debugging"
    )
    metrics_collection_enabled: bool = Field(
        default=True, description="Enable metrics collection"
    )

    # Configuration storage
    event_hubs: Dict[str, EventHubConfig] = Field(default_factory=dict)
    motherduck_configs: Dict[str, MotherDuckConfig] = Field(default_factory=dict)
    mappings: List[EventHubMotherDuckMapping] = Field(default_factory=list)

    def __init__(self, **kwargs):
        """Initialize configuration with dynamic parsing of environment variables."""
        super().__init__(**kwargs)
        self._parse_dynamic_config()

    def _parse_dynamic_config(self):
        """Parse dynamic Event Hub and MotherDuck configurations from environment variables."""
        env_vars = dict(os.environ)

        # Parse Event Hub configurations
        event_hub_pattern = re.compile(r"^EVENTHUBNAME_(\d+)$")
        event_hub_consumer_pattern = re.compile(r"^EVENTHUBNAME_(\d+)_CONSUMER_GROUP$")

        # First collect all Event Hub numbers and their consumer groups
        event_hub_data: dict[str, dict[str, str]] = {}

        for key, value in env_vars.items():
            match = event_hub_pattern.match(key)
            if match:
                hub_num = match.group(1)
                if hub_num not in event_hub_data:
                    event_hub_data[hub_num] = {}
                event_hub_data[hub_num]["name"] = value

            match = event_hub_consumer_pattern.match(key)
            if match:
                hub_num = match.group(1)
                if hub_num not in event_hub_data:
                    event_hub_data[hub_num] = {}
                event_hub_data[hub_num]["consumer_group"] = value

        # Create EventHubConfig instances with consumer groups
        for hub_num, data in event_hub_data.items():
            if "name" in data:  # Only create if we have a name
                if "consumer_group" not in data:
                    raise ValueError(
                        f"EVENTHUBNAME_{hub_num}_CONSUMER_GROUP is required for EVENTHUBNAME_{hub_num}"
                    )
                self.event_hubs[f"EVENTHUBNAME_{hub_num}"] = EventHubConfig(
                    name=data["name"],
                    namespace=self.eventhub_namespace,
                    consumer_group=data["consumer_group"],
                )

        # Parse MotherDuck configurations
        motherduck_keys: dict[str, dict[str, str]] = {}
        for key, value in env_vars.items():
            if key.startswith("MOTHERDUCK_") and "_" in key:
                parts = key.split("_", 2)
                if len(parts) >= 3:
                    md_num = parts[1]
                    setting = parts[2]

                    if md_num not in motherduck_keys:
                        motherduck_keys[md_num] = {}
                    motherduck_keys[md_num][setting.lower()] = value

        # Create MotherDuck configurations
        for md_num, settings in motherduck_keys.items():
            if all(key in settings for key in ["database", "schema", "table", "token"]):
                self.motherduck_configs[f"MOTHERDUCK_{md_num}"] = MotherDuckConfig(
                    database=settings["database"],
                    schema_name=settings["schema"],
                    table_name=settings["table"],
                    token=settings["token"],
                    batch_size=int(settings.get("batch", "1000")),
                )

        # Parse mappings - look for explicit mapping lines in env file
        # This is a simplified approach - in practice you might want more sophisticated parsing
        self._parse_mappings(env_vars)

    def _parse_mappings(self, env_vars: Dict[str, str]):
        """Parse mapping configurations from environment variables."""
        # Look for mapping patterns in comments or specific variables
        # For now, auto-map based on numbers: EVENTHUBNAME_1 -> MOTHERDUCK_1
        event_hub_nums = set()
        motherduck_nums = set()

        for key in env_vars.keys():
            if key.startswith("EVENTHUBNAME_"):
                num = key.split("_")[1]
                event_hub_nums.add(num)
            elif key.startswith("MOTHERDUCK_") and key.endswith("_DATABASE"):
                num = key.split("_")[1]
                motherduck_nums.add(num)

        # Create mappings for matching numbers
        for num in event_hub_nums:
            if num in motherduck_nums:
                eh_key = f"EVENTHUBNAME_{num}"
                md_key = f"MOTHERDUCK_{num}"
                if eh_key in self.event_hubs and md_key in self.motherduck_configs:
                    self.mappings.append(
                        EventHubMotherDuckMapping(
                            event_hub_key=eh_key,
                            motherduck_key=md_key,
                            channel_name_pattern="{event_hub}-{env}-{region}-{client_id}",
                        )
                    )

    @field_validator("eventhub_namespace")
    @classmethod
    def validate_eventhub_namespace(cls, v: str) -> str:
        """Validate Event Hub namespace format."""
        if not v.endswith(".servicebus.windows.net"):
            raise ValueError(
                "Event Hub namespace must end with .servicebus.windows.net"
            )
        return v

    @model_validator(mode="after")
    def validate_mappings_exist(self):
        """Validate that all mappings reference existing configurations."""
        for mapping in self.mappings:
            if mapping.event_hub_key not in self.event_hubs:
                raise ValueError(
                    f"Mapping references non-existent Event Hub: {mapping.event_hub_key}"
                )
            if mapping.motherduck_key not in self.motherduck_configs:
                raise ValueError(
                    f"Mapping references non-existent MotherDuck config: {mapping.motherduck_key}"
                )

        return self

    def get_event_hub_config(self, key: str) -> Optional[EventHubConfig]:
        """Get Event Hub configuration by key."""
        return self.event_hubs.get(key)

    def get_motherduck_config(self, key: str) -> Optional[MotherDuckConfig]:
        """Get MotherDuck configuration by key."""
        return self.motherduck_configs.get(key)

    def get_mapping_for_event_hub(
        self, event_hub_key: str
    ) -> Optional[EventHubMotherDuckMapping]:
        """Get mapping configuration for an Event Hub."""
        for mapping in self.mappings:
            if mapping.event_hub_key == event_hub_key:
                return mapping
        return None

    def generate_channel_name(self, event_hub_key: str, client_id: str) -> str:
        """
        Generate deterministic channel name for tracking and troubleshooting.

        Uses pattern: source-env-region-client-id for identification.
        """
        mapping = self.get_mapping_for_event_hub(event_hub_key)
        if not mapping:
            raise ValueError(f"No mapping found for Event Hub: {event_hub_key}")

        event_hub_config = self.get_event_hub_config(event_hub_key)
        if not event_hub_config:
            raise ValueError(f"No Event Hub config found: {event_hub_key}")

        return f"{event_hub_config.name}-{self.environment}-{self.region}-{client_id}"

    def validate_configuration(self) -> Dict[str, Any]:
        """
        Validate the complete configuration and return validation summary.

        Returns a dictionary with validation results including any warnings or issues.
        """
        warnings: List[str] = []
        errors: List[str] = []

        results = {
            "valid": True,
            "event_hubs_count": len(self.event_hubs),
            "motherduck_configs_count": len(self.motherduck_configs),
            "mappings_count": len(self.mappings),
            "warnings": warnings,
            "errors": errors,
        }

        # Check for unmapped configurations
        mapped_event_hubs = {m.event_hub_key for m in self.mappings}
        mapped_motherduck = {m.motherduck_key for m in self.mappings}

        unmapped_event_hubs = set(self.event_hubs.keys()) - mapped_event_hubs
        unmapped_motherduck = set(self.motherduck_configs.keys()) - mapped_motherduck

        if unmapped_event_hubs:
            warnings.append(f"Unmapped Event Hubs: {list(unmapped_event_hubs)}")

        if unmapped_motherduck:
            warnings.append(f"Unmapped MotherDuck configs: {list(unmapped_motherduck)}")

        return results


def load_config(env_file: Optional[str] = None) -> StreamDuckConfig:
    """
    Load configuration from environment file.

    Args:
        env_file: Optional path to .env file. Defaults to .env in current directory.

    Returns:
        Configured StreamDuckConfig instance.

    Raises:
        ValidationError: If configuration is invalid.
        FileNotFoundError: If specified env file doesn't exist.
    """
    if env_file:
        env_path = Path(env_file)
        if not env_path.exists():
            raise FileNotFoundError(f"Environment file not found: {env_file}")

        # Load environment variables from specified file
        try:
            from dotenv import load_dotenv

            load_dotenv(env_path)
        except ImportError:
            raise ImportError(
                "python-dotenv is required for loading .env files. Install it with: pip install python-dotenv"
            )

    # Get the eventhub_namespace from environment
    eventhub_namespace = os.getenv("EVENTHUB_NAMESPACE")
    if not eventhub_namespace:
        raise ValueError("EVENTHUB_NAMESPACE environment variable is required")

    return StreamDuckConfig(
        eventhub_namespace=eventhub_namespace,
        environment=os.getenv("ENVIRONMENT", "development"),
        region=os.getenv("REGION", "default"),
    )


# Example usage and testing utilities
if __name__ == "__main__":
    try:
        config = load_config()
        validation_results = config.validate_configuration()

        print("Configuration loaded successfully!")
        print(f"Event Hubs: {validation_results['event_hubs_count']}")
        print(f"MotherDuck Configs: {validation_results['motherduck_configs_count']}")
        print(f"Mappings: {validation_results['mappings_count']}")

        if validation_results["warnings"]:
            print("\nWarnings:")
            for warning in validation_results["warnings"]:
                print(f"  - {warning}")

    except Exception as e:
        print(f"Configuration error: {e}")
