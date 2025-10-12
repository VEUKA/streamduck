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
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import duckdb

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
        connection_config: Optional[MotherDuckConnectionConfig] = None,
        client_name_suffix: Optional[str] = None,
    ):
        self.motherduck_config = motherduck_config
        self.connection_config = connection_config
        self.client_name_suffix = client_name_suffix or str(uuid.uuid4())[:8]

        # Client components
        self.conn: Optional[duckdb.DuckDBPyConnection] = None

        # Statistics
        self.stats: Dict[str, Any] = {
            "client_created_at": None,
            "total_messages_sent": 0,
            "total_batches_sent": 0,
            "last_ingestion": None,
        }

    def start(self) -> None:
        """Initialize the MotherDuck streaming client."""
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

            self.stats["client_created_at"] = datetime.now(timezone.utc)
            logger.info(
                f"MotherDuck streaming client started: {self.client_name_suffix}"
            )

        except Exception as e:
            logger.error(f"Failed to start MotherDuck streaming client: {e}")
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

        except Exception as e:
            logger.error(f"Failed to ensure target table: {e}")
            raise

    def ingest_batch(
        self,
        channel_name: str,
        data_batch: List[Dict[str, Any]],
    ) -> bool:
        """
        Ingest a batch of data into MotherDuck.

        Args:
            channel_name: Name of the logical channel (for logging/tracking)
            data_batch: List of dictionaries containing data to ingest

        Returns:
            True if ingestion was successful, False otherwise
        """
        if not self.conn:
            raise RuntimeError("MotherDuck streaming client is not started")

        if not data_batch:
            logger.warning("Empty data batch provided for ingestion")
            return True

        try:
            logger.info(
                f"Ingesting {len(data_batch)} records to channel {channel_name}"
            )

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

            # Execute batch insert
            self.conn.execute(insert_query, params)

            # Update statistics
            self.stats["total_messages_sent"] += len(data_batch)
            self.stats["total_batches_sent"] += 1
            self.stats["last_ingestion"] = datetime.now(timezone.utc)

            logger.info(
                f"Successfully ingested {len(data_batch)} records to {channel_name}"
            )
            return True

        except Exception as e:
            logger.error(f"Error ingesting batch to {channel_name}: {e}", exc_info=True)
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

    def get_stats(self) -> Dict[str, Any]:
        """Get client statistics."""
        stats = self.stats.copy()

        # Calculate runtime
        if stats["client_created_at"] is not None:
            runtime = datetime.now(timezone.utc) - stats["client_created_at"]
            stats["runtime_seconds"] = runtime.total_seconds()

            if stats["total_messages_sent"] > 0:
                stats["messages_per_second"] = (
                    stats["total_messages_sent"] / runtime.total_seconds()
                )

        return stats

    def health_check(self) -> Dict[str, Any]:
        """Perform a health check of the streaming client."""
        health: Dict[str, Any] = {
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
    connection_config: Optional[MotherDuckConnectionConfig] = None,
    client_name_suffix: Optional[str] = None,
) -> MotherDuckStreamingClient:
    """Factory function to create a MotherDuck streaming client."""
    return MotherDuckStreamingClient(
        motherduck_config=motherduck_config,
        connection_config=connection_config,
        client_name_suffix=client_name_suffix,
    )


# Example usage
if __name__ == "__main__":
    import logging

    # Configure logging
    logging.basicConfig(level=logging.INFO)

    print("MotherDuck streaming client module loaded successfully")
    print("Use create_motherduck_streaming_client() to create client instances")
