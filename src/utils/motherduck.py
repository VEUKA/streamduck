"""
MotherDuck connection module for StreamDuck pipeline.

This module provides secure MotherDuck connection management using token authentication.
It includes configuration validation and connection testing capabilities.

Configuration is read from .env.motherduck file with the following required variables:
- MOTHERDUCK_TOKEN: Authentication token for MotherDuck
- MOTHERDUCK_DATABASE: Database name (optional, can be in connection string)
- TARGET_DB: Target database name
- TARGET_SCHEMA: Target schema name
- TARGET_TABLE: Target table name
"""

import json
import logging
from pathlib import Path

import duckdb
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings

# Configure logging
logger = logging.getLogger(__name__)


class MotherDuckConnectionConfig(BaseSettings):
    """
    MotherDuck connection configuration with token authentication.

    This class validates all required parameters for connecting to MotherDuck
    and provides connection management.
    """

    model_config = {
        "env_file": ".env.motherduck",
        "env_file_encoding": "utf-8",
        "case_sensitive": True,
        "extra": "ignore",
        "populate_by_name": True,
    }

    # Required connection parameters
    token: str = Field(..., description="MotherDuck authentication token", alias="MOTHERDUCK_TOKEN")
    database: str | None = Field(
        default=None,
        description="MotherDuck database name",
        alias="MOTHERDUCK_DATABASE",
    )
    target_db: str = Field(..., description="Target database name", alias="TARGET_DB")
    target_schema: str = Field(..., description="Target schema name", alias="TARGET_SCHEMA")
    target_table: str = Field(..., description="Target table name", alias="TARGET_TABLE")

    @field_validator("token")
    @classmethod
    def validate_token(cls, v: str) -> str:
        """Validate MotherDuck token is not empty."""
        if not v.strip():
            raise ValueError("MotherDuck token cannot be empty")
        return v.strip()

    @field_validator("database", "target_db", "target_schema", "target_table")
    @classmethod
    def validate_identifiers(cls, v: str | None) -> str | None:
        """Validate DuckDB identifiers."""
        if v is None:
            return None
        if not v.strip():
            raise ValueError("Identifier cannot be empty")
        return v.strip()

    def get_connection_string(self) -> str:
        """
        Get MotherDuck connection string.

        Returns:
            Connection string in format: md:database_name?motherduck_token=token
        """
        db_part = self.database if self.database else ""
        return f"md:{db_part}?motherduck_token={self.token}"


def validate_parameters(env_file: str | None = None) -> MotherDuckConnectionConfig:
    """
    Validate all required MotherDuck connection parameters.

    This function reads the configuration from the specified environment file
    (or .env.motherduck by default) and validates all required parameters using
    Pydantic settings.

    Args:
        env_file: Optional path to environment file. Defaults to .env.motherduck

    Returns:
        MotherDuckConnectionConfig: Validated configuration object

    Raises:
        ValidationError: If any required parameters are missing or invalid
        FileNotFoundError: If the environment file doesn't exist
    """
    # Create a custom config class if a specific env file is provided
    if env_file:
        env_path = Path(env_file)
        if not env_path.exists():
            raise FileNotFoundError(f"Environment file not found: {env_file}")

        class CustomMotherDuckConfig(MotherDuckConnectionConfig):
            model_config = {
                **MotherDuckConnectionConfig.model_config,
                "env_file": str(env_path),
            }

        return CustomMotherDuckConfig()  # type: ignore[call-arg]
    else:
        return MotherDuckConnectionConfig()  # type: ignore[call-arg]


def check_connection(
    config: MotherDuckConnectionConfig | None = None, env_file: str | None = None
) -> bool:
    """
    Test the MotherDuck connection with the provided configuration.

    This function creates a real connection to MotherDuck to verify that all
    connection parameters are correct and the service is accessible.

    Args:
        config: Optional MotherDuckConnectionConfig object. If not provided,
               configuration will be loaded from environment file.
        env_file: Optional path to environment file if config is not provided

    Returns:
        bool: True if connection is successful, False otherwise

    Raises:
        Exception: Re-raises any connection errors for detailed debugging
    """
    try:
        # Load configuration if not provided
        if config is None:
            config = validate_parameters(env_file)

        logger.info("Attempting to connect to MotherDuck...")

        # Get connection string
        conn_string = config.get_connection_string()

        # Create connection (masking token in logs)
        masked_conn = conn_string.split("?")[0] + "?motherduck_token=***"
        logger.info(f"Connection string: {masked_conn}")

        # Create connection
        conn = duckdb.connect(conn_string)

        try:
            # Test the connection with a simple query
            result = conn.execute("SELECT 'MotherDuck connection successful' as status").fetchone()

            if result:
                logger.info(f"Successfully connected to MotherDuck: {result[0]}")

                # Additional validation: check current database
                db_result = conn.execute("SELECT current_database()").fetchone()
                if db_result:
                    current_db = db_result[0]
                    logger.info(f"Current database: {current_db}")

                return True
            else:
                logger.error("Connection test query returned no result")
                return False

        finally:
            conn.close()

    except Exception as e:
        logger.error(f"MotherDuck connection error: {e}")
        raise


def get_connection(
    config: MotherDuckConnectionConfig | None = None, env_file: str | None = None
) -> duckdb.DuckDBPyConnection:
    """
    Get a MotherDuck connection object.

    This function returns a configured MotherDuck connection that can be used
    for executing queries and other database operations.

    Args:
        config: Optional MotherDuckConnectionConfig object. If not provided,
               configuration will be loaded from environment file.
        env_file: Optional path to environment file if config is not provided

    Returns:
        DuckDBPyConnection: Active MotherDuck connection object

    Raises:
        Exception: Any connection errors
    """
    # Load configuration if not provided
    if config is None:
        config = validate_parameters(env_file)

    # Get connection string
    conn_string = config.get_connection_string()

    # Create and return connection
    return duckdb.connect(conn_string)


def insert_partition_checkpoint(
    eventhub_namespace: str,
    eventhub: str,
    target_db: str,
    target_schema: str,
    target_table: str,
    partition_id: str,
    waterlevel: int,
    metadata: dict | None = None,
    conn: duckdb.DuckDBPyConnection | None = None,
    config: MotherDuckConnectionConfig | None = None,
    env_file: str | None = None,
) -> None:
    """
    Insert a checkpoint record for a specific partition.

    This function creates a new checkpoint record with partition-specific information.
    Uses schema with PARTITION_ID column and optional METADATA JSON column.

    Args:
        eventhub_namespace: EventHub namespace identifier
        eventhub: EventHub name
        target_db: Target database name
        target_schema: Target schema name
        target_table: Target table name
        partition_id: EventHub partition ID
        waterlevel: Water level (sequence number) for this partition
        metadata: Optional metadata dictionary (stored as JSON)
        conn: Optional existing DuckDB connection
        config: Optional MotherDuckConnectionConfig object
        env_file: Optional path to environment file

    Returns:
        None

    Raises:
        Exception: Any errors during insertion
    """
    should_close_conn = False
    if conn is None:
        conn = get_connection(config, env_file)
        should_close_conn = True

    try:
        # Get config for table name
        if config is None:
            config = validate_parameters(env_file)

        # Use the control table from environment variables (TARGET_DB, TARGET_SCHEMA, TARGET_TABLE)
        import os

        control_db = os.getenv("TARGET_DB", config.target_db)
        control_schema = os.getenv("TARGET_SCHEMA", config.target_schema)
        control_table = os.getenv("TARGET_TABLE", "ingestion_status")

        checkpoint_table = f"{control_db}.{control_schema}.{control_table}"

        # Prepare metadata as JSON string
        metadata_json = json.dumps(metadata) if metadata else None

        # Insert checkpoint record
        query = f"""
            INSERT INTO {checkpoint_table} (
                ts_inserted,
                eventhub_namespace,
                eventhub,
                target_db,
                target_schema,
                target_table,
                partition_id,
                waterlevel,
                metadata
            ) VALUES (
                CURRENT_TIMESTAMP,
                ?,
                ?,
                ?,
                ?,
                ?,
                ?,
                ?,
                ?
            )
        """

        conn.execute(
            query,
            [
                eventhub_namespace,
                eventhub,
                target_db,
                target_schema,
                target_table,
                partition_id,
                waterlevel,
                metadata_json,
            ],
        )

        logger.info(
            f"Partition checkpoint saved: partition={partition_id}, waterlevel={waterlevel}"
        )

    finally:
        if should_close_conn and conn:
            conn.close()


def get_partition_checkpoints(
    eventhub_namespace: str,
    eventhub: str,
    target_db: str,
    target_schema: str,
    target_table: str,
    conn: duckdb.DuckDBPyConnection | None = None,
    config: MotherDuckConnectionConfig | None = None,
    env_file: str | None = None,
) -> dict[str, int] | None:
    """
    Retrieve the latest checkpoint for each partition.

    Args:
        eventhub_namespace: EventHub namespace identifier
        eventhub: EventHub name
        target_db: Target database name
        target_schema: Target schema name
        target_table: Target table name
        conn: Optional existing DuckDB connection
        config: Optional MotherDuckConnectionConfig object
        env_file: Optional path to environment file

    Returns:
        Dictionary mapping partition_id to waterlevel, or None if no checkpoints found

    Raises:
        Exception: Any errors during query execution
    """
    should_close_conn = False
    if conn is None:
        conn = get_connection(config, env_file)
        should_close_conn = True

    try:
        if config is None:
            config = validate_parameters(env_file)

        # Use the control table from environment variables (TARGET_DB, TARGET_SCHEMA, TARGET_TABLE)
        # not the ingestion target
        import os

        control_db = os.getenv("TARGET_DB", config.target_db)
        control_schema = os.getenv("TARGET_SCHEMA", config.target_schema)
        control_table = os.getenv("TARGET_TABLE", "ingestion_status")

        checkpoint_table = f"{control_db}.{control_schema}.{control_table}"

        # Query to get MAXIMUM waterlevel (sequence number) per partition
        # This ensures we always resume from the highest sequence number seen,
        # regardless of timestamp ordering issues or clock skew
        query = f"""
            SELECT
                partition_id,
                MAX(waterlevel) as waterlevel
            FROM {checkpoint_table}
            WHERE eventhub_namespace = ?
              AND eventhub = ?
              AND target_db = ?
              AND target_schema = ?
              AND target_table = ?
              AND partition_id IS NOT NULL
            GROUP BY partition_id
        """

        result = conn.execute(
            query,
            [eventhub_namespace, eventhub, target_db, target_schema, target_table],
        ).fetchall()

        if not result:
            logger.info(
                f"No partition checkpoints found for {eventhub_namespace}/{eventhub} -> {target_db}.{target_schema}.{target_table}"
            )
            return None

        # Convert to dictionary
        partition_checkpoints = {row[0]: row[1] for row in result}

        logger.info(
            f"Retrieved partition checkpoints: {partition_checkpoints} for {eventhub_namespace}/{eventhub}"
        )
        return partition_checkpoints

    finally:
        if should_close_conn and conn:
            conn.close()


def create_control_table(
    target_db: str,
    target_schema: str,
    target_table: str,
    conn: duckdb.DuckDBPyConnection | None = None,
    config: MotherDuckConnectionConfig | None = None,
    env_file: str | None = None,
) -> bool:
    """
    Create the control/waterlevel table if it doesn't exist.

    This table is used for storing EventHub checkpoint information (waterlevel)
    for each partition to track processing progress.

    Args:
        target_db: Target database name
        target_schema: Target schema name (e.g., 'control')
        target_table: Target table name (e.g., 'waterleveleh')
        conn: Optional existing DuckDB connection
        config: Optional MotherDuckConnectionConfig object
        env_file: Optional path to environment file

    Returns:
        True if table was created/verified successfully, False otherwise
    """
    should_close_conn = False
    if conn is None:
        try:
            conn = get_connection(config, env_file)
            should_close_conn = True
        except Exception as e:
            logger.error(f"Failed to get connection for control table creation: {e}")
            return False

    try:
        # Create schema if it doesn't exist
        schema_name = f"{target_db}.{target_schema}"
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
        logger.info(f"Schema verified: {schema_name}")

        # Create control/waterlevel table
        table_name = f"{target_db}.{target_schema}.{target_table}"
        create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                ts_inserted TIMESTAMP,
                eventhub_namespace VARCHAR NOT NULL,
                eventhub VARCHAR NOT NULL,
                target_db VARCHAR NOT NULL,
                target_schema VARCHAR NOT NULL,
                target_table VARCHAR NOT NULL,
                partition_id VARCHAR NOT NULL,
                waterlevel BIGINT NOT NULL,
                metadata JSON
            )
        """

        conn.execute(create_table_sql)
        logger.info(f"Control table created/verified: {table_name}")

        return True

    except Exception as e:
        logger.error(f"Failed to create control table: {e}")
        return False
    finally:
        if should_close_conn and conn:
            conn.close()


def create_ingestion_status_table(
    conn: duckdb.DuckDBPyConnection | None = None,
    config: MotherDuckConnectionConfig | None = None,
    env_file: str | None = None,
) -> None:
    """
    Create the ingestion_status table if it doesn't exist.

    Args:
        conn: Optional existing DuckDB connection
        config: Optional MotherDuckConnectionConfig object
        env_file: Optional path to environment file
    """
    should_close_conn = False
    if conn is None:
        conn = get_connection(config, env_file)
        should_close_conn = True

    try:
        if config is None:
            config = validate_parameters(env_file)

        # Create schema if it doesn't exist
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {config.target_db}.{config.target_schema}")

        # Create table
        table_name = f"{config.target_db}.{config.target_schema}.ingestion_status"
        create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                ts_inserted TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                eventhub_namespace VARCHAR,
                eventhub VARCHAR,
                target_db VARCHAR,
                target_schema VARCHAR,
                target_table VARCHAR,
                partition_id VARCHAR,
                waterlevel BIGINT,
                metadata JSON
            )
        """

        conn.execute(create_table_sql)
        logger.info(f"Ingestion status table created/verified: {table_name}")

    finally:
        if should_close_conn and conn:
            conn.close()


# Example usage and testing
if __name__ == "__main__":
    import logging

    # Configure logging for testing
    logging.basicConfig(level=logging.INFO)

    try:
        # Example 1: Validate parameters
        print("Validating MotherDuck connection parameters...")
        config = validate_parameters()
        print("✓ Configuration validation successful")

        # Example 2: Test connection
        print("Testing MotherDuck connection...")
        if check_connection(config):
            print("✓ Connection test successful")
        else:
            print("✗ Connection test failed")

    except Exception as e:
        print(f"✗ Error: {e}")
