"""
Pipeline orchestrator for StreamDuck pipeline.

This module coordinates multiple EventHub consumers and MotherDuck streaming clients
to provide a complete data pipeline from Azure Event Hubs to MotherDuck.

The orchestrator:
1. Manages multiple EventHub->MotherDuck mapping pairs concurrently
2. Coordinates EventHub consumers with MotherDuck streaming clients
3. Provides comprehensive monitoring and health checking
4. Handles graceful shutdown and resource cleanup
5. Offers runtime statistics and status reporting
"""

import asyncio
import logging
import signal
from datetime import UTC, datetime
from typing import Any

import logfire

from consumers.eventhub import (
    EventHubAsyncConsumer,
    EventHubMessage,
)
from streaming.motherduck import (
    MotherDuckStreamingClient,
    create_motherduck_streaming_client,
)
from utils.config import EventHubMotherDuckMapping, StreamDuckConfig
from utils.motherduck import MotherDuckConnectionConfig

logger = logging.getLogger(__name__)


class PipelineMapping:
    """Represents a single EventHub -> MotherDuck mapping with its components."""

    def __init__(
        self,
        mapping_config: EventHubMotherDuckMapping,
        pipeline_config: StreamDuckConfig,
        motherduck_connection_config: MotherDuckConnectionConfig | None = None,
        retry_manager: Any | None = None,
    ):
        self.mapping_config = mapping_config
        self.pipeline_config = pipeline_config
        self.motherduck_connection_config = motherduck_connection_config
        self.retry_manager = retry_manager

        # Get component configurations
        self.eventhub_config = pipeline_config.get_event_hub_config(mapping_config.event_hub_key)
        self.motherduck_config = pipeline_config.get_motherduck_config(
            mapping_config.motherduck_key
        )

        if not self.eventhub_config or not self.motherduck_config:
            raise ValueError(f"Invalid mapping configuration: {mapping_config}")

        # Initialize components
        self.eventhub_consumer: EventHubAsyncConsumer | None = None
        self.motherduck_client: MotherDuckStreamingClient | None = None
        self.running = False

        # Statistics
        self.stats: dict[str, Any] = {
            "mapping_key": f"{mapping_config.event_hub_key}->{mapping_config.motherduck_key}",
            "started_at": None,
            "messages_processed": 0,
            "batches_processed": 0,
            "last_activity": None,
            "errors": [],
        }

    def start(self) -> None:
        """Start the mapping components."""
        if self.running:
            logger.warning(f"Mapping {self.stats['mapping_key']} is already running")
            return

        logger.info(f"Starting mapping: {self.stats['mapping_key']}")

        # Validate configurations
        if not self.eventhub_config or not self.motherduck_config:
            raise ValueError("Missing EventHub or MotherDuck configuration")

        try:
            # Start MotherDuck streaming client
            self.motherduck_client = create_motherduck_streaming_client(
                motherduck_config=self.motherduck_config,
                connection_config=self.motherduck_connection_config,
                retry_manager=self.retry_manager,
            )
            self.motherduck_client.start()

            # Create message processor that uses MotherDuck client
            def message_processor(messages: list[EventHubMessage]) -> bool:
                return self._process_messages(messages)

            # Create EventHub consumer (synchronous creation)
            self.eventhub_consumer = EventHubAsyncConsumer(
                eventhub_config=self.eventhub_config,
                target_db=self.motherduck_config.database,
                target_schema=self.motherduck_config.schema_name,
                target_table=self.motherduck_config.table_name or "events",
                message_processor=message_processor,
                motherduck_config=self.motherduck_connection_config,
                batch_size=self.motherduck_config.batch_size,
            )

            self.running = True
            self.stats["started_at"] = datetime.now(UTC)

            logger.info(f"Mapping {self.stats['mapping_key']} started successfully")

        except Exception as e:
            logger.error(f"Failed to start mapping {self.stats['mapping_key']}: {e}")
            # Don't await here, handle in async context
            if self.motherduck_client:
                self.motherduck_client.stop()
            raise

    async def start_async(self) -> None:
        """Start the async components (EventHub consumer)."""
        if not self.running or not self.eventhub_consumer:
            raise RuntimeError("Mapping must be started before starting async components")

        logger.info(f"Starting async components for mapping: {self.stats['mapping_key']}")

        # Wrap consumer execution in a span to show active mapping in hierarchy
        with logfire.span(
            f"mapping.{self.stats['mapping_key']}",
            event_hub=self.eventhub_config.name if self.eventhub_config else "unknown",
            motherduck_table=self.motherduck_config.table_name
            if self.motherduck_config
            else "unknown",
            batch_size=self.motherduck_config.batch_size if self.motherduck_config else 0,
        ):
            await self.eventhub_consumer.start()

    async def stop(self) -> None:
        """Stop the mapping components gracefully."""
        if not self.running:
            return

        logger.info(f"🛑 Stopping mapping: {self.stats['mapping_key']}")
        self.running = False

        # Stop EventHub consumer (this will process remaining messages and save checkpoints)
        if self.eventhub_consumer:
            logger.info(f"📦 Finalizing EventHub consumer for {self.stats['mapping_key']}...")
            await self.eventhub_consumer.stop()
            self.eventhub_consumer = None

        # Stop MotherDuck client
        if self.motherduck_client:
            logger.info(f"🔌 Closing MotherDuck client for {self.stats['mapping_key']}...")
            self.motherduck_client.stop()
            self.motherduck_client = None

        logger.info(f"✅ Mapping {self.stats['mapping_key']} stopped gracefully")

    def _process_messages(self, messages: list[EventHubMessage]) -> bool:
        """Process a batch of EventHub messages by sending them to MotherDuck."""
        with logfire.span(
            "orchestrator.process_messages",
            message_count=len(messages),
            mapping_key=self.stats["mapping_key"],
        ) as span:
            logger.info(f"🔧 _process_messages called with {len(messages)} messages")

            if not self.motherduck_client or not self.eventhub_config:
                logger.error(
                    "❌ MotherDuck client or EventHub config not available for message processing"
                )
                span.set_attribute("error", "missing_client_or_config")
                span.set_attribute("success", False)
                return False

            try:
                logger.info(f"📊 Converting {len(messages)} messages to dict format...")
                # Convert messages to data format
                data_batch = [msg.to_dict() for msg in messages]
                logger.info(f"✅ Converted {len(data_batch)} messages")

                # Log first message as sample
                if data_batch:
                    logger.info(f"📝 Sample message: {str(data_batch[0])[:200]}...")

                # Ingest data with channel name (for logging/tracking)
                channel_name = f"{self.eventhub_config.namespace}/{self.eventhub_config.name}"
                span.set_attribute("channel_name", channel_name)

                logger.info(
                    f"📤 Sending batch of {len(data_batch)} messages to MotherDuck (channel: {channel_name})..."
                )
                success: bool = self.motherduck_client.ingest_batch(
                    channel_name=channel_name,
                    data_batch=data_batch,
                )

                if success:
                    self.stats["messages_processed"] += len(messages)
                    self.stats["batches_processed"] += 1
                    self.stats["last_activity"] = datetime.now(UTC)

                    span.set_attribute("success", True)
                    span.set_attribute("total_messages_processed", self.stats["messages_processed"])
                    span.set_attribute("total_batches_processed", self.stats["batches_processed"])

                    logger.info(
                        f"✅ Batch ingestion successful! "
                        f"Total messages processed: {self.stats['messages_processed']}, "
                        f"Total batches: {self.stats['batches_processed']}"
                    )
                    logfire.info(
                        "Batch processing complete",
                        messages=len(messages),
                        total_processed=self.stats["messages_processed"],
                        mapping=self.stats["mapping_key"],
                    )
                else:
                    logger.error("❌ MotherDuck batch ingestion returned False")
                    span.set_attribute("success", False)
                    logfire.error("Batch ingestion failed", message_count=len(messages))

                return success

            except Exception as e:
                error_msg = f"Error processing messages: {e}"
                logger.error(error_msg, exc_info=True)  # Added exc_info=True for full traceback
                span.set_attribute("error", str(e))
                span.set_attribute("success", False)
                logfire.error("Message processing error", error=str(e), message_count=len(messages))
                self.stats["errors"].append(
                    {
                        "timestamp": datetime.now(UTC).isoformat(),
                        "error": error_msg,
                    }
                )
                return False

    def get_stats(self) -> dict[str, Any]:
        """Get mapping statistics."""
        stats = self.stats.copy()

        # Add component statistics
        if self.eventhub_consumer:
            stats["eventhub"] = self.eventhub_consumer.get_stats()

        if self.motherduck_client:
            stats["motherduck"] = self.motherduck_client.get_stats()

        # Calculate runtime
        if stats["started_at"]:
            runtime = datetime.now(UTC) - stats["started_at"]
            stats["runtime_seconds"] = runtime.total_seconds()

        return stats

    def health_check(self) -> dict[str, Any]:
        """Perform health check on mapping components."""
        health = {
            "mapping_key": self.stats["mapping_key"],
            "overall_status": "stopped",
            "eventhub_status": "stopped",
            "motherduck_status": "stopped",
            "errors": [],
        }

        if not self.running:
            return health

        try:
            health["overall_status"] = "running"

            # Check EventHub consumer
            if self.eventhub_consumer:
                consumer_stats = self.eventhub_consumer.get_stats()
                health["eventhub_status"] = (
                    "running" if consumer_stats.get("start_time") else "stopped"
                )
                health["eventhub_details"] = consumer_stats

            # Check MotherDuck client
            if self.motherduck_client:
                md_health = self.motherduck_client.health_check()
                health["motherduck_status"] = md_health["client_status"]
                health["motherduck_details"] = md_health

        except Exception as e:
            health["errors"].append(f"Health check error: {e}")

        return health


class PipelineOrchestrator:
    """
    Main pipeline orchestrator that manages multiple EventHub->MotherDuck mappings.

    This orchestrator:
    - Manages multiple mapping pairs concurrently
    - Provides centralized monitoring and health checking
    - Handles graceful shutdown and resource cleanup
    - Offers comprehensive statistics and status reporting
    """

    def __init__(self, config: StreamDuckConfig, retry_manager: Any | None = None):
        self.config = config
        self.retry_manager = retry_manager
        self.mappings: list[PipelineMapping] = []
        self.running = False
        self.start_time: datetime | None = None

        # Async components
        self.loop: asyncio.AbstractEventLoop | None = None
        self.tasks: set[asyncio.Task] = set()

        # Statistics
        self.stats: dict[str, Any] = {
            "total_mappings": 0,
            "active_mappings": 0,
            "total_messages_processed": 0,
            "total_batches_processed": 0,
            "start_time": None,
            "errors": [],
        }

        # Initialize MotherDuck connection config
        self.motherduck_connection_config: MotherDuckConnectionConfig | None = None

    def initialize(self) -> None:
        """Initialize the pipeline orchestrator."""
        logger.info("Initializing pipeline orchestrator...")

        try:
            # Note: MotherDuck connection is token-based, no need for validate_parameters
            # Connection config is optional and will be created per-client if needed
            logger.info("MotherDuck connection will use token-based authentication")

            # Initialize mappings
            for mapping_config in self.config.mappings:
                pipeline_mapping = PipelineMapping(
                    mapping_config=mapping_config,
                    pipeline_config=self.config,
                    motherduck_connection_config=self.motherduck_connection_config,
                    retry_manager=self.retry_manager,
                )
                self.mappings.append(pipeline_mapping)

            self.stats["total_mappings"] = len(self.mappings)
            logger.info(f"Initialized {len(self.mappings)} pipeline mappings")

        except Exception as e:
            logger.error(f"Failed to initialize pipeline orchestrator: {e}")
            raise

    def start(self) -> None:
        """Start the pipeline orchestrator."""
        if self.running:
            logger.warning("Pipeline orchestrator is already running")
            return

        logger.info("Starting pipeline orchestrator...")

        try:
            # Start all mappings
            for mapping in self.mappings:
                mapping.start()

            self.running = True
            self.start_time = datetime.now(UTC)
            self.stats["start_time"] = self.start_time
            self.stats["active_mappings"] = len(self.mappings)

            logger.info(f"Pipeline orchestrator started with {len(self.mappings)} mappings")

        except Exception as e:
            logger.error(f"Failed to start pipeline orchestrator: {e}")
            # Handle sync cleanup
            for mapping in self.mappings:
                if mapping.motherduck_client:
                    mapping.motherduck_client.stop()
            raise

    async def run_async(self) -> None:
        """Run the async components of the pipeline."""
        with logfire.span(
            "orchestrator.run_async",
            total_mappings=len(self.mappings),
            active_mappings=self.stats.get("active_mappings", 0),
        ) as span:
            if not self.running:
                raise RuntimeError(
                    "Pipeline orchestrator must be started before running async components"
                )

            logger.info("Starting async pipeline components...")

            try:
                # Start async components for each mapping
                for mapping in self.mappings:
                    task = asyncio.create_task(mapping.start_async())
                    self.tasks.add(task)

                span.set_attribute("tasks_started", len(self.tasks))
                logfire.info("Pipeline async components started", tasks=len(self.tasks))

                # Wait for all tasks (they should run indefinitely until stopped)
                if self.tasks:
                    # Keep checking if we're still running
                    while self.running:
                        done, _pending = await asyncio.wait(
                            self.tasks, timeout=1.0, return_when=asyncio.FIRST_EXCEPTION
                        )

                        # Check for exceptions in completed tasks
                        for task in done:
                            exc = task.exception()
                            if exc is not None:
                                logger.error(f"Task failed with exception: {exc}")
                                logfire.error("Pipeline task failed", error=str(exc))
                                span.set_attribute("task_error", str(exc))
                                self.running = False
                                raise exc

            except asyncio.CancelledError:
                logger.info("Async pipeline cancelled")
                logfire.info("Pipeline cancelled")
                span.set_attribute("cancelled", True)
            except Exception as e:
                logger.error(f"Error in async pipeline execution: {e}")
                logfire.error("Pipeline execution error", error=str(e))
                span.set_attribute("error", str(e))
                raise
            finally:
                # IMPORTANT: Always stop mappings BEFORE cancelling tasks
                # This allows graceful shutdown to process remaining messages
                # regardless of how we got here (normal stop, signal, or exception)
                logger.info("📦 Finalizing pipeline before cancellation...")
                logfire.info("Finalizing pipeline")
                for mapping in self.mappings:
                    try:
                        await mapping.stop()
                    except Exception as e:
                        logger.error(f"Error stopping mapping during finalization: {e}")
                        logfire.error("Mapping stop error", error=str(e))
                self.running = False

            # Clean up tasks
            for task in self.tasks:
                if not task.done():
                    task.cancel()

            # Wait a bit for tasks to cancel
            if self.tasks:
                await asyncio.gather(*self.tasks, return_exceptions=True)
            self.tasks.clear()

    async def stop(self) -> None:
        """Stop the pipeline orchestrator gracefully."""
        if not self.running:
            return

        logger.info("🛑 Stopping pipeline orchestrator gracefully...")
        self.running = False

        # Stop all mappings first (this will process remaining messages and update checkpoints)
        logger.info(f"📦 Stopping {len(self.mappings)} mappings and finalizing checkpoints...")
        for mapping in self.mappings:
            try:
                await mapping.stop()
            except Exception as e:
                logger.error(f"Error stopping mapping: {e}", exc_info=True)

        # Cancel all async tasks
        for task in self.tasks:
            if not task.done():
                task.cancel()

        # Wait for tasks to complete
        if self.tasks:
            await asyncio.gather(*self.tasks, return_exceptions=True)
        self.tasks.clear()

        self.stats["active_mappings"] = 0
        logger.info("✅ Pipeline orchestrator stopped gracefully")

    def get_stats(self) -> dict[str, Any]:
        """Get comprehensive pipeline statistics."""
        stats = self.stats.copy()

        # Calculate runtime
        if self.start_time:
            runtime = datetime.now(UTC) - self.start_time
            stats["runtime_seconds"] = runtime.total_seconds()

        # Aggregate mapping statistics
        total_messages = 0
        total_batches = 0
        mapping_stats = []

        for mapping in self.mappings:
            mapping_stat = mapping.get_stats()
            mapping_stats.append(mapping_stat)
            total_messages += mapping_stat.get("messages_processed", 0)
            total_batches += mapping_stat.get("batches_processed", 0)

        stats["total_messages_processed"] = total_messages
        stats["total_batches_processed"] = total_batches
        stats["mappings"] = mapping_stats

        # Calculate throughput
        if stats.get("runtime_seconds", 0) > 0:
            stats["messages_per_second"] = total_messages / stats["runtime_seconds"]
            stats["batches_per_second"] = total_batches / stats["runtime_seconds"]

        return stats

    def health_check(self) -> dict[str, Any]:
        """Perform comprehensive health check."""
        health: dict[str, Any] = {
            "orchestrator_status": "running" if self.running else "stopped",
            "total_mappings": len(self.mappings),
            "healthy_mappings": 0,
            "errors": [],
            "mappings": [],
        }

        try:
            healthy_count = 0
            for mapping in self.mappings:
                mapping_health = mapping.health_check()
                health["mappings"].append(mapping_health)

                if mapping_health["overall_status"] == "running":
                    healthy_count += 1

            health["healthy_mappings"] = healthy_count

        except Exception as e:
            health["errors"].append(f"Health check error: {e}")

        return health

    def setup_signal_handlers(self) -> None:
        """Setup signal handlers for graceful shutdown."""

        def signal_handler(signum, frame):
            logger.info(f"🛑 Received signal {signum} (Ctrl+C), initiating graceful shutdown...")
            logger.info("📦 Processing remaining messages and updating checkpoints...")
            self.running = False
            # Don't cancel tasks immediately - let stop() handle graceful shutdown
            # Just raise KeyboardInterrupt to break out of the run loop
            raise KeyboardInterrupt()

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)


async def run_pipeline(config: StreamDuckConfig, retry_manager: Any | None = None) -> None:
    """
    Main function to run the complete pipeline.

    This function:
    1. Initializes the pipeline orchestrator
    2. Starts all components
    3. Runs the async components
    4. Handles graceful shutdown

    Args:
        config: Pipeline configuration
        retry_manager: Optional retry manager for intelligent retry logic
    """
    orchestrator = PipelineOrchestrator(config, retry_manager=retry_manager)

    try:
        # Initialize and start
        orchestrator.initialize()
        orchestrator.start()

        # Setup signal handlers for graceful shutdown
        orchestrator.setup_signal_handlers()

        # Run async components
        await orchestrator.run_async()

    except KeyboardInterrupt:
        logger.info("Pipeline interrupted by user")
    except Exception as e:
        logger.error(f"Pipeline error: {e}", exc_info=True)
    finally:
        # Ensure clean shutdown
        await orchestrator.stop()


# Example usage
if __name__ == "__main__":
    import logging

    # Configure logging
    logging.basicConfig(level=logging.INFO)

    print("Pipeline orchestrator module loaded successfully")
    print("Use run_pipeline() with ELTSnowHPConfig to start the pipeline")
