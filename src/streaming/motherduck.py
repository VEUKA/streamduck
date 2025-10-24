"""
MotherDuck streaming client for high-performance data ingestion.

This module provides a streaming client that:
1. Uses batched INSERTs for high-performance data ingestion to MotherDuck
2. Manages connections with proper lifecycle
3. Handles batch processing with configurable timeouts
4. Integrates with existing MotherDuck connection configuration
5. Provides comprehensive error handling and recovery

Based on DuckDB's efficient batch ingestion patterns optimized for MotherDuck.
"""

import logging
import uuid
from datetime import UTC, datetime
from typing import Any

import duckdb
import logfire

from utils.config import MotherDuckConfig
from utils.motherduck import MotherDuckConnectionConfig

logger = logging.getLogger(__name__)


class MotherDuckStreamingClient:
    """
    High-performance MotherDuck streaming client.

    This client:
    - Uses batched INSERTs for optimal performance
    - Manages connections with proper lifecycle
    - Handles batch ingestion with monitoring
    - Provides error recovery and status tracking
    """

    def __init__(
        self,
        motherduck_config: MotherDuckConfig,
        connection_config: MotherDuckConnectionConfig | None = None,
        client_name_suffix: str | None = None,
        retry_manager: Any | None = None,
    ):
        self.motherduck_config = motherduck_config
        self.connection_config = connection_config
        self.client_name_suffix = client_name_suffix or str(uuid.uuid4())[:8]
        self.retry_manager = retry_manager

        # Client components
        self.conn: duckdb.DuckDBPyConnection | None = None
        self._ingest_with_retry: Any  # Callable that may be decorated with retry logic

        # Statistics
        self.stats: dict[str, Any] = {
            "client_created_at": None,
            "total_messages_sent": 0,
            "total_batches_sent": 0,
            "last_ingestion": None,
            "retry_stats": {
                "total_retries": 0,
                "successful_retries": 0,
                "failed_retries": 0,
            },
        }

        # Apply retry decorator to implementation if retry manager is provided
        if self.retry_manager:
            decorator = self.retry_manager.get_retry_decorator()
            # Store decorated function as callable attribute
            self._ingest_with_retry = decorator(self._ingest_batch_impl)  # type: ignore[method-assign]
        else:
            self._ingest_with_retry = self._ingest_batch_impl  # type: ignore[assignment]

    def start(self) -> None:
        """Initialize the MotherDuck streaming client."""
        with logfire.span(
            "motherduck.client.start",
            database=self.motherduck_config.database,
            schema=self.motherduck_config.schema_name,
            table=self.motherduck_config.table_name,
            client_suffix=self.client_name_suffix,
        ):
            if self.conn is not None:
                logger.warning("MotherDuck streaming client is already started")
                return

            logger.info(
                f"Starting MotherDuck streaming client for {self.motherduck_config.database}.{self.motherduck_config.schema_name}.{self.motherduck_config.table_name}"
            )

            try:
                # Get connection string from config
                if self.connection_config:
                    conn_string = self.connection_config.get_connection_string()
                else:
                    # Build connection string from motherduck_config
                    if not hasattr(self.motherduck_config, "token"):
                        raise ValueError("MotherDuck token not configured")
                    token = self.motherduck_config.token
                    database = self.motherduck_config.database
                    conn_string = f"md:{database}?motherduck_token={token}"

                # Create connection
                self.conn = duckdb.connect(conn_string)

                # Ensure target table exists
                self._ensure_target_table()

                self.stats["client_created_at"] = datetime.now(UTC)
                logger.info(f"MotherDuck streaming client started: {self.client_name_suffix}")
                logfire.info(
                    "MotherDuck client started successfully", client_suffix=self.client_name_suffix
                )

            except Exception as e:
                logger.error(f"Failed to start MotherDuck streaming client: {e}")
                logfire.error("Failed to start MotherDuck client", error=str(e))
                self.stop()
                raise

    def stop(self) -> None:
        """Stop the MotherDuck streaming client and clean up resources."""
        logger.info("Stopping MotherDuck streaming client...")

        # Close connection
        if self.conn:
            try:
                self.conn.close()
            except Exception as e:
                logger.error(f"Error closing MotherDuck connection: {e}")
            finally:
                self.conn = None

        logger.info("MotherDuck streaming client stopped")

    def _ensure_target_table(self) -> None:
        """Ensure the target table exists with the correct schema."""
        with logfire.span(
            "motherduck.ensure_table",
            database=self.motherduck_config.database,
            schema=self.motherduck_config.schema_name,
            table=self.motherduck_config.table_name,
        ):
            if not self.conn:
                raise RuntimeError("Connection not established")

            try:
                # Create schema if it doesn't exist
                schema_ddl = f"""
                    CREATE SCHEMA IF NOT EXISTS {self.motherduck_config.database}.{self.motherduck_config.schema_name}
                """
                self.conn.execute(schema_ddl)

                # Create table if it doesn't exist
                table_name = f"{self.motherduck_config.database}.{self.motherduck_config.schema_name}.{self.motherduck_config.table_name}"

                # Default schema for EventHub messages
                table_ddl = f"""
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        event_body VARCHAR,
                        partition_id VARCHAR,
                        sequence_number BIGINT,
                        enqueued_time TIMESTAMP,
                        properties JSON,
                        system_properties JSON,
                        ingestion_timestamp TIMESTAMP
                    )
                """
                self.conn.execute(table_ddl)
                logger.info(f"Target table verified: {table_name}")
                logfire.info("Target table verified", table=table_name)

            except Exception as e:
                logger.error(f"Failed to ensure target table: {e}")
                logfire.error("Failed to ensure target table", error=str(e))
                raise

    def _ingest_batch_impl(
        self,
        channel_name: str,
        data_batch: list[dict[str, Any]],
    ) -> bool:
        """
        Internal implementation of batch ingestion.
        This method will be wrapped with retry decorator if configured.

        Args:
            channel_name: Name of the logical channel (for logging/tracking)
            data_batch: List of dictionaries containing data to ingest

        Returns:
            True if ingestion was successful

        Raises:
            RuntimeError: If client is not started or ingestion fails
        """
        with logfire.span(
            "motherduck.ingest_batch",
            channel_name=channel_name,
            batch_size=len(data_batch),
            database=self.motherduck_config.database,
            table=self.motherduck_config.table_name,
        ) as span:
            if not self.conn:
                raise RuntimeError("MotherDuck streaming client is not started")

            if not data_batch:
                logger.warning("Empty data batch provided for ingestion")
                span.set_attribute("empty_batch", True)
                return True

            logger.info(f"Ingesting {len(data_batch)} records to channel {channel_name}")

            # Build table name
            table_name = f"{self.motherduck_config.database}.{self.motherduck_config.schema_name}.{self.motherduck_config.table_name}"

            # Prepare batch insert
            # Convert data_batch to values for INSERT
            placeholders = ", ".join(["(?, ?, ?, ?, ?, ?, ?)"] * len(data_batch))
            insert_query = f"""
                INSERT INTO {table_name} (
                    event_body,
                    partition_id,
                    sequence_number,
                    enqueued_time,
                    properties,
                    system_properties,
                    ingestion_timestamp
                ) VALUES {placeholders}
            """

            # Flatten the data for parameterized query
            params = []
            for row in data_batch:
                params.extend(
                    [
                        row.get("event_body"),
                        row.get("partition_id"),
                        row.get("sequence_number"),
                        row.get("enqueued_time"),
                        row.get("properties"),
                        row.get("system_properties"),
                        row.get("ingestion_timestamp"),
                    ]
                )

            # Execute batch insert (will raise exception on failure)
            self.conn.execute(insert_query, params)

            # Update statistics
            self.stats["total_messages_sent"] += len(data_batch)
            self.stats["total_batches_sent"] += 1
            self.stats["last_ingestion"] = datetime.now(UTC)

            # Track ingestion metrics in span
            span.set_attribute("messages_sent", len(data_batch))
            span.set_attribute("total_messages", self.stats["total_messages_sent"])
            span.set_attribute("total_batches", self.stats["total_batches_sent"])

            logger.info(f"Successfully ingested {len(data_batch)} records to {channel_name}")
            logfire.info(
                "Batch ingested successfully",
                channel=channel_name,
                records=len(data_batch),
                total_messages=self.stats["total_messages_sent"],
            )
            return True

    def ingest_batch(
        self,
        channel_name: str,
        data_batch: list[dict[str, Any]],
    ) -> bool:
        """
        Public method to ingest a batch of data into MotherDuck.

        This method calls the internal implementation which may be wrapped
        with retry logic if a retry manager is configured.

        Args:
            channel_name: Name of the logical channel (for logging/tracking)
            data_batch: List of dictionaries containing data to ingest

        Returns:
            True if ingestion was successful, False otherwise
        """
        with logfire.span(
            "motherduck.ingest_batch_with_retry",
            channel_name=channel_name,
            batch_size=len(data_batch),
            has_retry_manager=self.retry_manager is not None,
        ) as span:
            try:
                result: bool = self._ingest_with_retry(channel_name, data_batch)
                span.set_attribute("success", result)
                return result
            except Exception as e:
                # After all retries exhausted (or no retry configured)
                logger.error(
                    f"❌ Batch ingestion FAILED after all retry attempts: {e}",
                    exc_info=True,
                )
                logfire.error(
                    "Batch ingestion failed after retries",
                    channel=channel_name,
                    batch_size=len(data_batch),
                    error=str(e),
                )
                span.set_attribute("success", False)
                span.set_attribute("error", str(e))
                self.stats["retry_stats"]["failed_retries"] += 1
                return False

    def create_channel_name(
        self,
        eventhub_name: str,
        environment: str = "dev",
        region: str = "default",
    ) -> str:
        """
        Create a deterministic channel name for troubleshooting.

        Format: {eventhub}-{env}-{region}-{client_suffix}
        """
        return f"{eventhub_name}-{environment}-{region}-{self.client_name_suffix}"

    def get_stats(self) -> dict[str, Any]:
        """Get client statistics."""
        stats = self.stats.copy()

        # Calculate runtime
        if stats["client_created_at"] is not None:
            runtime = datetime.now(UTC) - stats["client_created_at"]
            runtime_seconds = runtime.total_seconds()
            stats["runtime_seconds"] = runtime_seconds

            if stats["total_messages_sent"] > 0 and runtime_seconds > 0:
                stats["messages_per_second"] = stats["total_messages_sent"] / runtime_seconds
            elif stats["total_messages_sent"] > 0:
                stats["messages_per_second"] = 0.0

        return stats

    def health_check(self) -> dict[str, Any]:
        """Perform a health check of the streaming client."""
        health: dict[str, Any] = {
            "client_status": "stopped",
            "connection_active": False,
            "errors": [],
        }

        try:
            if self.conn is None:
                health["errors"].append("Connection not initialized")
                return health

            # Test connection with a simple query
            try:
                result = self.conn.execute("SELECT 1").fetchone()
                if result and result[0] == 1:
                    health["client_status"] = "running"
                    health["connection_active"] = True
                else:
                    health["errors"].append("Connection test failed")
            except Exception as e:
                health["errors"].append(f"Connection test error: {e}")

        except Exception as e:
            health["errors"].append(f"Health check error: {e}")

        return health


def create_motherduck_streaming_client(
    motherduck_config: MotherDuckConfig,
    connection_config: MotherDuckConnectionConfig | None = None,
    client_name_suffix: str | None = None,
    retry_manager: Any | None = None,
) -> MotherDuckStreamingClient:
    """Factory function to create a MotherDuck streaming client."""
    return MotherDuckStreamingClient(
        motherduck_config=motherduck_config,
        connection_config=connection_config,
        client_name_suffix=client_name_suffix,
        retry_manager=retry_manager,
    )


# Example usage
if __name__ == "__main__":
    import logging

    # Configure logging
    logging.basicConfig(level=logging.INFO)

    print("MotherDuck streaming client module loaded successfully")
    print("Use create_motherduck_streaming_client() to create client instances")
