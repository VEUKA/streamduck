"""
Snowflake connection module for ELT Snowflake High Performance pipeline.

This module provides secure Snowflake connection management using JWT authentication
with private key files. It includes configuration validation and connection testing
capabilities.

Configuration is read from .env.snowflake file with the following required variables:
- SNOWFLAKE_ACCOUNT: Account identifier
- SNOWFLAKE_USER: Username
- SNOWFLAKE_PRIVATE_KEY_FILE: Path to private key file
- SNOWFLAKE_PRIVATE_KEY_PASSWORD: Private key password
- SNOWFLAKE_WAREHOUSE: Warehouse name
- SNOWFLAKE_DATABASE: Database name
- SNOWFLAKE_SCHEMA: Schema name
- TARGET_DB: Target database name
- TARGET_SCHEMA: Target schema name
- TARGET_TABLE: Target table name
"""

import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, Optional

import snowflake.connector as sc
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings
from snowflake.snowpark import Session
from snowflake.snowpark.functions import current_timestamp

# Configure logging
logger = logging.getLogger(__name__)


class SnowflakeConnectionConfig(BaseSettings):
    """
    Snowflake connection configuration with JWT authentication.

    This class validates all required parameters for connecting to Snowflake
    using private key authentication and provides connection management.
    """

    model_config = {
        "env_file": ".env.snowflake",
        "env_file_encoding": "utf-8",
        "case_sensitive": True,
        "extra": "ignore",
        "populate_by_name": True,
    }

    # Required connection parameters
    account: str = Field(
        ..., description="Snowflake account identifier", alias="SNOWFLAKE_ACCOUNT"
    )
    user: str = Field(..., description="Snowflake username", alias="SNOWFLAKE_USER")
    private_key_file: str = Field(
        ..., description="Path to private key file", alias="SNOWFLAKE_PRIVATE_KEY_FILE"
    )
    private_key_password: Optional[str] = Field(
        default=None,
        description="Private key password (None for unencrypted keys)",
        alias="SNOWFLAKE_PRIVATE_KEY_PASSWORD",
    )
    warehouse: str = Field(
        ..., description="Snowflake warehouse name", alias="SNOWFLAKE_WAREHOUSE"
    )
    database: str = Field(
        ..., description="Snowflake database name", alias="SNOWFLAKE_DATABASE"
    )
    schema_name: str = Field(
        ..., description="Snowflake schema name", alias="SNOWFLAKE_SCHEMA"
    )
    target_db: str = Field(..., description="Target database name", alias="TARGET_DB")
    target_schema: str = Field(
        ..., description="Target schema name", alias="TARGET_SCHEMA"
    )
    target_table: str = Field(
        ..., description="Target table name", alias="TARGET_TABLE"
    )

    # Optional parameters with defaults
    authenticator: str = Field(
        default="SNOWFLAKE_JWT",
        description="Authentication method",
        alias="SNOWFLAKE_AUTHENTICATOR",
    )
    role: Optional[str] = Field(
        default=None, description="Snowflake role", alias="SNOWFLAKE_ROLE"
    )

    @field_validator("private_key_file")
    @classmethod
    def validate_private_key_file_exists(cls, v: str) -> str:
        """Validate that the private key file exists and is readable."""
        key_path = Path(v).expanduser().resolve()
        if not key_path.exists():
            raise ValueError(f"Private key file not found: {v}")
        if not key_path.is_file():
            raise ValueError(f"Private key path is not a file: {v}")
        if not os.access(key_path, os.R_OK):
            raise ValueError(f"Private key file is not readable: {v}")
        return str(key_path)

    @field_validator("account")
    @classmethod
    def validate_account_format(cls, v: str) -> str:
        """Validate Snowflake account identifier format."""
        if not v.strip():
            raise ValueError("Account identifier cannot be empty")
        # Basic validation - Snowflake accounts typically contain letters, numbers, and hyphens
        if not all(c.isalnum() or c in "-._" for c in v):
            raise ValueError("Account identifier contains invalid characters")
        return v.strip()

    @field_validator(
        "user",
        "warehouse",
        "database",
        "schema_name",
        "target_db",
        "target_schema",
        "target_table",
    )
    @classmethod
    def validate_snowflake_identifiers(cls, v: str) -> str:
        """Validate Snowflake identifiers (user, warehouse, database, schema)."""
        if not v.strip():
            raise ValueError("Snowflake identifier cannot be empty")
        # Snowflake identifiers can contain letters, numbers, underscores, and dollar signs
        v_clean = v.strip()
        if not v_clean.replace("_", "").replace("$", "").replace("-", "").isalnum():
            raise ValueError(f"Invalid Snowflake identifier: {v}")
        return v_clean

    @field_validator("authenticator")
    @classmethod
    def validate_authenticator(cls, v: str) -> str:
        """Validate authenticator type."""
        valid_authenticators = [
            "SNOWFLAKE_JWT",
            "SNOWFLAKE",
            "EXTERNALBROWSER",
            "OAUTH",
        ]
        if v.upper() not in valid_authenticators:
            raise ValueError(
                f"Invalid authenticator. Must be one of: {valid_authenticators}"
            )
        return v.upper()

    def to_connection_params(self) -> Dict[str, Any]:
        """
        Convert configuration to snowflake.connector connection parameters.

        Returns:
            Dictionary of connection parameters suitable for snowflake.connector.connect()
        """
        params = {
            "account": self.account,
            "user": self.user,
            "authenticator": self.authenticator,
            "private_key_file": self.private_key_file,
            "warehouse": self.warehouse,
            "database": self.database,
            "schema": self.schema_name,
        }

        # Only include password if it's provided (for encrypted keys)
        if self.private_key_password:
            params["private_key_file_pwd"] = self.private_key_password

        if self.role:
            params["role"] = self.role

        return params


def validate_parameters(env_file: Optional[str] = None) -> SnowflakeConnectionConfig:
    """
    Validate all required Snowflake connection parameters.

    This function reads the configuration from the specified environment file
    (or .env.snowflake by default) and validates all required parameters using
    Pydantic settings.

    Args:
        env_file: Optional path to environment file. Defaults to .env.snowflake

    Returns:
        SnowflakeConnectionConfig: Validated configuration object

    Raises:
        ValidationError: If any required parameters are missing or invalid
        FileNotFoundError: If the environment file doesn't exist
    """
    # Create a custom config class if a specific env file is provided
    if env_file:
        env_path = Path(env_file)
        if not env_path.exists():
            raise FileNotFoundError(f"Environment file not found: {env_file}")

        class CustomSnowflakeConfig(SnowflakeConnectionConfig):
            model_config = {
                **SnowflakeConnectionConfig.model_config,
                "env_file": str(env_path),
            }

        return CustomSnowflakeConfig()  # type: ignore[call-arg]
    else:
        return SnowflakeConnectionConfig()  # type: ignore[call-arg]


def check_connection(
    config: Optional[SnowflakeConnectionConfig] = None, env_file: Optional[str] = None
) -> bool:
    """
    Test the Snowflake connection with the provided configuration.

    This function creates a real connection to Snowflake to verify that all
    connection parameters are correct and the service is accessible.

    Args:
        config: Optional SnowflakeConnectionConfig object. If not provided,
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

        logger.info(f"Attempting to connect to Snowflake account: {config.account}")

        # Get connection parameters
        conn_params = config.to_connection_params()

        # Create connection
        ctx = sc.connect(**conn_params)

        # Test the connection with a simple query
        cs = ctx.cursor()
        try:
            # Simple query to test connection
            cs.execute("SELECT CURRENT_VERSION()")
            result = cs.fetchone()

            if result:
                logger.info(
                    f"Successfully connected to Snowflake. Version: {result[0]}"
                )

                # Additional validation: check current context
                cs.execute(
                    "SELECT CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_WAREHOUSE()"
                )
                context = cs.fetchone()

                if context:
                    db, schema, warehouse = context
                    logger.info(
                        f"Current context - Database: {db}, Schema: {schema}, Warehouse: {warehouse}"
                    )

                    # Verify we're in the expected database and schema
                    if db != config.database.upper():
                        logger.warning(
                            f"Connected to different database: {db} (expected: {config.database})"
                        )
                    if schema != config.schema_name.upper():
                        logger.warning(
                            f"Connected to different schema: {schema} (expected: {config.schema_name})"
                        )
                    if warehouse != config.warehouse.upper():
                        logger.warning(
                            f"Connected to different warehouse: {warehouse} (expected: {config.warehouse})"
                        )

                return True
            else:
                logger.error("Connection test query returned no result")
                return False

        finally:
            cs.close()
            ctx.close()

    except sc.errors.DatabaseError as e:
        logger.error(f"Snowflake database error: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error connecting to Snowflake: {e}")
        raise


def get_connection(
    config: Optional[SnowflakeConnectionConfig] = None, env_file: Optional[str] = None
) -> sc.SnowflakeConnection:
    """
    Get a Snowflake connection object.

    This function returns a configured Snowflake connection that can be used
    for executing queries and other database operations.

    Args:
        config: Optional SnowflakeConnectionConfig object. If not provided,
               configuration will be loaded from environment file.
        env_file: Optional path to environment file if config is not provided

    Returns:
        SnowflakeConnection: Active Snowflake connection object

    Raises:
        Exception: Any connection errors
    """
    # Load configuration if not provided
    if config is None:
        config = validate_parameters(env_file)

    # Get connection parameters
    conn_params = config.to_connection_params()

    # Create and return connection
    return sc.connect(**conn_params)


def get_snowpark_session(
    config: Optional[SnowflakeConnectionConfig] = None, env_file: Optional[str] = None
) -> Session:
    """
    Get a Snowpark Session object.

    This function returns a configured Snowpark Session that can be used
    for DataFrame operations and other Snowpark functionality.

    Args:
        config: Optional SnowflakeConnectionConfig object. If not provided,
               configuration will be loaded from environment file.
        env_file: Optional path to environment file if config is not provided

    Returns:
        Session: Active Snowpark Session object

    Raises:
        Exception: Any connection errors
    """
    # Load configuration if not provided
    if config is None:
        config = validate_parameters(env_file)

    # Build connection parameters for Snowpark
    connection_parameters = {
        "account": config.account,
        "user": config.user,
        "authenticator": config.authenticator,
        "private_key_file": config.private_key_file,
        "warehouse": config.warehouse,
        "database": config.database,
        "schema": config.schema_name,
    }

    # Add optional parameters
    if config.private_key_password:
        connection_parameters["private_key_file_pwd"] = config.private_key_password

    if config.role:
        connection_parameters["role"] = config.role

    # Create and return Snowpark session
    return Session.builder.configs(connection_parameters).create()


def insert_ingestion_status(
    eventhub_namespace: str,
    eventhub: str,
    target_db: str,
    target_schema: str,
    target_table: str,
    waterlevel: int | str,  # Accept both int (legacy) and str (JSON for per-partition)
    session: Optional[Session] = None,
    config: Optional[SnowflakeConnectionConfig] = None,
    env_file: Optional[str] = None,
    partition_id: str = "0",  # Default partition for backward compatibility
    metadata: Optional[dict] = None,
) -> None:
    """
    Insert a new record into the INGESTION_STATUS table using Snowpark.

    This is a legacy compatibility wrapper around insert_partition_checkpoint().
    For new code, prefer using insert_partition_checkpoint() directly.

    Args:
        eventhub_namespace: EventHub namespace identifier
        eventhub: EventHub name
        target_db: Target database name
        target_schema: Target schema name
        target_table: Target table name
        waterlevel: Water level value (sequence number)
        session: Optional existing Snowpark Session
        config: Optional SnowflakeConnectionConfig object
        env_file: Optional path to environment file
        partition_id: Partition ID (default "0" for legacy compatibility)
        metadata: Optional metadata dictionary

    Returns:
        None

    Raises:
        Exception: Any errors during insertion
    """
    # Delegate to the new per-partition function
    insert_partition_checkpoint(
        eventhub_namespace=eventhub_namespace,
        eventhub=eventhub,
        target_db=target_db,
        target_schema=target_schema,
        target_table=target_table,
        partition_id=partition_id,
        waterlevel=int(waterlevel) if isinstance(waterlevel, str) else waterlevel,
        metadata=metadata,
        session=session,
        config=config,
        env_file=env_file,
    )


def get_ingestion_status(
    eventhub_namespace: str,
    eventhub: str,
    target_db: str,
    target_schema: str,
    target_table: str,
    session: Optional[Session] = None,
    config: Optional[SnowflakeConnectionConfig] = None,
    env_file: Optional[str] = None,
) -> Optional[tuple[Any, int | str]]:
    """
    Retrieve the latest ingestion status (timestamp and waterlevel) for the specified parameters.

    This function queries the INGESTION_STATUS table and returns the most recent
    timestamp and waterlevel for the given EventHub and target table combination.

    Note: The returned timestamp is in the session's local timezone. The table uses
    TIMESTAMP_LTZ which stores UTC internally but returns values in session timezone.
    To get UTC time, use timestamp.utctimetuple() or convert using timezone libraries.

    The waterlevel can be either int (legacy format) or str (JSON with per-partition checkpoints).

    Args:
        eventhub_namespace: EventHub namespace identifier
        eventhub: EventHub name
        target_db: Target database name
        target_schema: Target schema name
        target_table: Target table name
        session: Optional existing Snowpark Session. If not provided, a new session will be created.
        config: Optional SnowflakeConnectionConfig object (used if session is not provided)
        env_file: Optional path to environment file (used if session is not provided)

    Returns:
        Optional tuple containing (timestamp, waterlevel) for the latest record,
        or None if no matching record is found.
        Note: timestamp is in session timezone, not UTC.

    Raises:
        Exception: Any errors during query execution
    """
    # Create session if not provided
    should_close_session = False
    if session is None:
        session = get_snowpark_session(config, env_file)
        should_close_session = True

    try:
        # Get the configuration to determine which database schema contains INGESTION_STATUS
        if config is None:
            config = validate_parameters(env_file)

        # Construct the fully qualified table name
        ingestion_table = f"{config.database}.{config.schema_name}.INGESTION_STATUS"

        # Query the table
        df = session.table(ingestion_table)

        # Filter by the provided parameters
        df = df.filter(
            (df["EVENTHUB_NAMESPACE"] == eventhub_namespace)
            & (df["EVENTHUB"] == eventhub)
            & (df["TARGET_DB"] == target_db)
            & (df["TARGET_SCHEMA"] == target_schema)
            & (df["TARGET_TABLE"] == target_table)
        )

        # Order by timestamp descending to get the latest record
        df = df.order_by(df["TS_INSERTED"].desc())

        # Select only the timestamp and waterlevel columns
        df = df.select("TS_INSERTED", "WATERLEVEL")

        # Get the first row (most recent)
        result = df.first()

        if result:
            timestamp = result["TS_INSERTED"]
            waterlevel = result["WATERLEVEL"]
            logger.info(
                f"Retrieved ingestion status for {eventhub_namespace}/{eventhub} -> {target_db}.{target_schema}.{target_table}: "
                f"timestamp={timestamp}, waterlevel={waterlevel}"
            )
            return (timestamp, waterlevel)
        else:
            logger.info(
                f"No ingestion status found for {eventhub_namespace}/{eventhub} -> {target_db}.{target_schema}.{target_table}"
            )
            return None

    finally:
        # Close session if we created it
        if should_close_session:
            session.close()


def insert_partition_checkpoint(
    eventhub_namespace: str,
    eventhub: str,
    target_db: str,
    target_schema: str,
    target_table: str,
    partition_id: str,
    waterlevel: int,
    metadata: Optional[dict] = None,
    session: Optional[Session] = None,
    config: Optional[SnowflakeConnectionConfig] = None,
    env_file: Optional[str] = None,
) -> None:
    """
    Insert a checkpoint record for a specific partition.

    This function creates a new checkpoint record with partition-specific information.
    Uses the improved schema with PARTITION_ID column and optional METADATA variant.

    Args:
        eventhub_namespace: EventHub namespace identifier
        eventhub: EventHub name
        target_db: Target database name
        target_schema: Target schema name
        target_table: Target table name
        partition_id: EventHub partition ID
        waterlevel: Water level (sequence number) for this partition
        metadata: Optional metadata dictionary (stored as VARIANT)
        session: Optional existing Snowpark Session
        config: Optional SnowflakeConnectionConfig object
        env_file: Optional path to environment file

    Returns:
        None

    Raises:
        Exception: Any errors during insertion
    """
    should_close_session = False
    if session is None:
        session = get_snowpark_session(config, env_file)
        should_close_session = True

    try:
        # Create a DataFrame with the data to insert
        data = [
            (
                eventhub_namespace,
                eventhub,
                target_db,
                target_schema,
                target_table,
                partition_id,
                waterlevel,
            )
        ]

        df = session.create_dataframe(
            data,
            schema=[
                "EVENTHUB_NAMESPACE",
                "EVENTHUB",
                "TARGET_DB",
                "TARGET_SCHEMA",
                "TARGET_TABLE",
                "PARTITION_ID",
                "WATERLEVEL",
            ],
        )

        # Add current timestamp
        df = df.with_column("TS_INSERTED", current_timestamp())

        # Add metadata as VARIANT if provided
        from snowflake.snowpark.functions import lit, parse_json

        if metadata:
            metadata_json = json.dumps(metadata)
            df = df.with_column("METADATA", parse_json(lit(metadata_json)))
        else:
            df = df.with_column("METADATA", lit(None))

        # Get config for table name
        if config is None:
            config = validate_parameters(env_file)

        ingestion_table = f"{config.database}.{config.schema_name}.INGESTION_STATUS"

        # Reorder columns to match table schema
        df = df.select(
            "TS_INSERTED",
            "EVENTHUB_NAMESPACE",
            "EVENTHUB",
            "TARGET_DB",
            "TARGET_SCHEMA",
            "TARGET_TABLE",
            "WATERLEVEL",
            "PARTITION_ID",
            "METADATA",
        )

        # Write to table
        df.write.mode("append").save_as_table(ingestion_table)

        logger.info(
            f"Partition checkpoint saved: partition={partition_id}, waterlevel={waterlevel}"
        )

    finally:
        if should_close_session and session:
            session.close()


def get_partition_checkpoints(
    eventhub_namespace: str,
    eventhub: str,
    target_db: str,
    target_schema: str,
    target_table: str,
    session: Optional[Session] = None,
    config: Optional[SnowflakeConnectionConfig] = None,
    env_file: Optional[str] = None,
) -> Optional[dict[str, int]]:
    """
    Retrieve the latest checkpoint for each partition.

    Args:
        eventhub_namespace: EventHub namespace identifier
        eventhub: EventHub name
        target_db: Target database name
        target_schema: Target schema name
        target_table: Target table name
        session: Optional existing Snowpark Session
        config: Optional SnowflakeConnectionConfig object
        env_file: Optional path to environment file

    Returns:
        Dictionary mapping partition_id to waterlevel, or None if no checkpoints found

    Raises:
        Exception: Any errors during query execution
    """
    should_close_session = False
    if session is None:
        session = get_snowpark_session(config, env_file)
        should_close_session = True

    try:
        if config is None:
            config = validate_parameters(env_file)

        ingestion_table = f"{config.database}.{config.schema_name}.INGESTION_STATUS"

        # Get the table
        df = session.table(ingestion_table)

        # Filter by the provided parameters
        df = df.filter(
            (df["EVENTHUB_NAMESPACE"] == eventhub_namespace)
            & (df["EVENTHUB"] == eventhub)
            & (df["TARGET_DB"] == target_db)
            & (df["TARGET_SCHEMA"] == target_schema)
            & (df["TARGET_TABLE"] == target_table)
            & (df["PARTITION_ID"].is_not_null())  # Only get partition-specific records
        )

        # Get the latest record for each partition using window function
        from snowflake.snowpark import Window
        from snowflake.snowpark.functions import desc, row_number

        window_spec = Window.partition_by("PARTITION_ID").order_by(desc("TS_INSERTED"))
        df = df.with_column("row_num", row_number().over(window_spec))
        df = df.filter(df["row_num"] == 1)

        # Select only partition_id and waterlevel
        df = df.select("PARTITION_ID", "WATERLEVEL")

        # Collect results
        results = df.collect()

        if not results:
            logger.info(
                f"No partition checkpoints found for {eventhub_namespace}/{eventhub} -> {target_db}.{target_schema}.{target_table}"
            )
            return None

        # Convert to dictionary
        partition_checkpoints = {
            row["PARTITION_ID"]: row["WATERLEVEL"] for row in results
        }

        logger.info(
            f"Retrieved partition checkpoints: {partition_checkpoints} for {eventhub_namespace}/{eventhub}"
        )
        return partition_checkpoints

    finally:
        if should_close_session and session:
            session.close()


# Example usage and testing
if __name__ == "__main__":
    import logging

    # Configure logging for testing
    logging.basicConfig(level=logging.INFO)

    try:
        # Example 1: Validate parameters
        print("Validating Snowflake connection parameters...")
        config = validate_parameters()
        print("✓ Configuration validation successful")

        # Example 2: Test connection
        print("Testing Snowflake connection...")
        if check_connection(config):
            print("✓ Connection test successful")
        else:
            print("✗ Connection test failed")

    except Exception as e:
        print(f"✗ Error: {e}")
