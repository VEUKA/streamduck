"""
Azure EventHub async consumer with custom MotherDuck-based checkpoint management.

This module provides an EventHub consumer that:
1. Receives messages asynchronously from EventHub partitions
2. Batches messages for efficient processing (1000 messages or 5 minutes)
3. Uses MotherDuck tables for checkpoint storage instead of blob storage
4. Integrates with the existing MotherDuck connection utilities
5. Provides robust error handling and recovery mechanisms

Based on Azure EventHub SDK patterns but adapted for MotherDuck checkpointing.
"""

import asyncio
import json
import logging
import time
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Set

from azure.eventhub import EventData
from azure.eventhub.aio import (
    CheckpointStore,
    EventHubConsumerClient,
    PartitionContext,
)
from azure.identity.aio import DefaultAzureCredential

from utils.config import EventHubConfig
from utils.motherduck import MotherDuckConnectionConfig

logger = logging.getLogger(__name__)


class BytesEncoder(json.JSONEncoder):
    """Custom JSON encoder that converts bytes to strings."""

    def default(self, obj):
        if isinstance(obj, bytes):
            return obj.decode("utf-8", errors="replace")
        return super().default(obj)


def _convert_bytes_to_str(obj: Any) -> Any:
    """
    Recursively convert bytes to strings in nested structures.
    Handles dicts, lists, tuples, and bytes objects.
    """
    if isinstance(obj, bytes):
        return obj.decode("utf-8", errors="replace")
    elif isinstance(obj, dict):
        return {
            _convert_bytes_to_str(k): _convert_bytes_to_str(v) for k, v in obj.items()
        }
    elif isinstance(obj, (list, tuple)):
        return type(obj)(_convert_bytes_to_str(item) for item in obj)
    else:
        return obj


class EventHubMessage:
    """Wrapper for EventHub message with additional metadata."""

    def __init__(self, event_data: EventData, partition_id: str, sequence_number: int):
        self.event_data = event_data
        self.partition_id = partition_id
        self.sequence_number = sequence_number
        self.body = event_data.body_as_str()
        self.enqueued_time = event_data.enqueued_time
        self.properties = event_data.properties
        self.system_properties = event_data.system_properties
        self.partition_context: Optional[PartitionContext] = (
            None  # For checkpoint updates
        )

    def to_dict(self) -> Dict[str, Any]:
        """Convert message to dictionary for MotherDuck ingestion."""
        # Convert properties and system_properties, handling bytes values recursively
        properties_json = None
        if self.properties:
            clean_props = _convert_bytes_to_str(self.properties)
            properties_json = json.dumps(clean_props, cls=BytesEncoder)

        system_props_json = None
        if self.system_properties:
            clean_sys_props = _convert_bytes_to_str(dict(self.system_properties))
            system_props_json = json.dumps(clean_sys_props, cls=BytesEncoder)

        result = {
            "event_body": self.body,
            "partition_id": self.partition_id,
            "sequence_number": self.sequence_number,
            "enqueued_time": self.enqueued_time.isoformat()
            if self.enqueued_time
            else None,
            "properties": properties_json,
            "system_properties": system_props_json,
            "ingestion_timestamp": datetime.now(timezone.utc).isoformat(),
        }

        # Debug: Check for any remaining bytes in the result
        for key, value in result.items():
            if isinstance(value, bytes):
                value_preview = value[:100] if len(value) > 100 else value
                logger.error(
                    f"FOUND BYTES in to_dict() result: key={key}, value_type={type(value)}, value={value_preview!r}"
                )

        return result


class MessageBatch:
    """Container for batched EventHub messages."""

    def __init__(self, max_size: int = 1000, max_wait_seconds: int = 300):
        self.messages: List[EventHubMessage] = []
        self.max_size = max_size
        self.max_wait_seconds = max_wait_seconds
        self.created_at = time.time()
        self.last_sequence_by_partition: Dict[str, int] = {}

    def add_message(self, message: EventHubMessage) -> bool:
        """
        Add message to batch.

        Returns True if batch is ready for processing after adding this message.
        """
        self.messages.append(message)
        self.last_sequence_by_partition[message.partition_id] = message.sequence_number

        # Check if batch is ready
        return self.is_ready()

    def is_ready(self) -> bool:
        """Check if batch is ready for processing."""
        return (
            len(self.messages) >= self.max_size
            or (time.time() - self.created_at) >= self.max_wait_seconds
        )

    def get_checkpoint_data(self) -> Dict[str, int]:
        """Get checkpoint data (highest sequence number per partition)."""
        return self.last_sequence_by_partition.copy()

    def to_dict_list(self) -> List[Dict[str, Any]]:
        """Convert all messages to list of dictionaries."""
        return [msg.to_dict() for msg in self.messages]


class MotherDuckCheckpointManager:
    """Manages checkpoints using MotherDuck tables."""

    def __init__(
        self,
        eventhub_namespace: str,
        eventhub_name: str,
        target_db: str,
        target_schema: str,
        target_table: str,
        motherduck_config: Optional[MotherDuckConnectionConfig] = None,
        session=None,
    ):
        self.eventhub_namespace = eventhub_namespace
        self.eventhub_name = eventhub_name
        self.target_db = target_db
        self.target_schema = target_schema
        self.target_table = target_table
        self.motherduck_config = motherduck_config
        self.session = session
        self._external_session = session is not None

    async def get_last_checkpoint(self) -> Optional[Dict[str, int]]:
        """
        Get the last checkpoint from MotherDuck for each partition.

        Returns:
            Dictionary mapping partition_id to last processed sequence_number,
            or None if no checkpoint exists.
        """
        try:
            from utils.motherduck import get_partition_checkpoints

            result: Optional[Dict[str, int]] = get_partition_checkpoints(
                eventhub_namespace=self.eventhub_namespace,
                eventhub=self.eventhub_name,
                target_db=self.target_db,
                target_schema=self.target_schema,
                target_table=self.target_table,
                conn=self.session,
                config=self.motherduck_config,
            )

            if result:
                logger.info(f"Loaded per-partition checkpoints: {result}")
            else:
                logger.info("No checkpoints found, starting from beginning")

            return result

        except Exception as e:
            logger.error(f"Failed to get last checkpoint: {e}", exc_info=True)
            return None

    async def save_checkpoint(
        self,
        partition_checkpoints: Dict[str, int],
        partition_metadata: Optional[Dict[str, Dict[str, Any]]] = None,
    ) -> bool:
        """
        Save per-partition checkpoints to MotherDuck.

        Args:
            partition_checkpoints: Dictionary mapping partition_id to offset (int)
            partition_metadata: Optional dict mapping partition_id to metadata dict
                               (e.g., {"0": {"sequence_number": 3582, "timestamp": "..."}})
        """
        try:
            from utils.motherduck import insert_partition_checkpoint

            # Save each partition checkpoint as a separate row
            for partition_id, waterlevel in partition_checkpoints.items():
                # Get metadata for this partition (if provided)
                metadata = None
                if partition_metadata and partition_id in partition_metadata:
                    metadata = partition_metadata[partition_id]

                logger.info(
                    f"📝 Inserting checkpoint: partition={partition_id}, "
                    f"waterlevel={waterlevel} (type={type(waterlevel).__name__}), "
                    f"metadata={metadata}"
                )

                insert_partition_checkpoint(
                    eventhub_namespace=self.eventhub_namespace,
                    eventhub=self.eventhub_name,
                    target_db=self.target_db,
                    target_schema=self.target_schema,
                    target_table=self.target_table,
                    partition_id=partition_id,
                    waterlevel=waterlevel,
                    metadata=metadata,  # Now includes sequence_number and other info
                    conn=self.session,
                    config=self.motherduck_config,
                )

            logger.info(
                f"Checkpoint saved for {len(partition_checkpoints)} partitions: {partition_checkpoints}"
            )
            return True
        except Exception as e:
            logger.error(f"Failed to save checkpoint: {e}", exc_info=True)
            return False

    def close(self):
        """Close resources if we own them."""
        if self.session and not self._external_session:
            self.session.close()


class MotherDuckCheckpointStore(CheckpointStore):
    """
    Azure SDK-compatible checkpoint store that uses MotherDuck for persistence.

    This class implements the Azure EventHub CheckpointStore abstract base class,
    bridging the Azure SDK's checkpoint mechanism with our MotherDuck storage.
    """

    def __init__(self, checkpoint_manager: MotherDuckCheckpointManager):
        """
        Initialize the checkpoint store.

        Args:
            checkpoint_manager: The MotherDuck checkpoint manager instance
        """
        self.checkpoint_manager = checkpoint_manager
        # Cache for ownership claims (partition_id -> dict with ownership info)
        self._ownership_cache: Dict[str, Dict[str, Any]] = {}
        # Cache for checkpoints (partition_id -> dict with checkpoint info)
        self._checkpoint_cache: Dict[str, Dict[str, Any]] = {}

    async def list_ownership(
        self, fully_qualified_namespace: str, eventhub_name: str, consumer_group: str
    ) -> List[Dict[str, Any]]:
        """
        List all partition ownership records.

        Returns:
            List of ownership dictionaries with keys:
                - fully_qualified_namespace
                - eventhub_name
                - consumer_group
                - partition_id
                - owner_id
                - last_modified_time
                - etag
        """
        # Return cached ownership records
        return list(self._ownership_cache.values())

    async def claim_ownership(
        self, ownership_list: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Claim ownership of partitions.

        Args:
            ownership_list: List of ownership dictionaries to claim

        Returns:
            List of successfully claimed ownership dictionaries
        """
        claimed = []
        for ownership in ownership_list:
            partition_id = ownership["partition_id"]
            # Update cache with ownership info
            self._ownership_cache[partition_id] = {
                "fully_qualified_namespace": ownership["fully_qualified_namespace"],
                "eventhub_name": ownership["eventhub_name"],
                "consumer_group": ownership["consumer_group"],
                "partition_id": partition_id,
                "owner_id": ownership["owner_id"],
                "last_modified_time": time.time(),
                "etag": str(time.time()),  # Simple etag using timestamp
            }
            claimed.append(self._ownership_cache[partition_id])

        logger.debug(f"Claimed ownership for {len(claimed)} partitions")
        return claimed

    async def update_checkpoint(self, checkpoint: Dict[str, Any]) -> None:
        """
        Update checkpoint for a partition.

        Args:
            checkpoint: Dictionary with keys:
                - fully_qualified_namespace
                - eventhub_name
                - consumer_group
                - partition_id
                - offset (string - this is the EventHub offset, NOT sequence number)
                - sequence_number
        """
        partition_id = checkpoint["partition_id"]
        offset = checkpoint["offset"]  # This is the EventHub offset (string)
        sequence_number = checkpoint["sequence_number"]

        # Log what we received from SDK
        logger.info(
            f"🔍 SDK update_checkpoint called: partition={partition_id}, "
            f"offset={offset!r} (type={type(offset).__name__}), "
            f"sequence={sequence_number} (type={type(sequence_number).__name__})"
        )

        # Update checkpoint cache
        self._checkpoint_cache[partition_id] = checkpoint

        # CRITICAL: We need to save the OFFSET, not sequence number!
        # EventHub uses offset for resumption, not sequence number
        # Convert offset string to int for storage (MotherDuck waterlevel column is BIGINT)
        try:
            offset_int = int(offset)
            logger.info(
                f"✅ Converted offset to int: partition={partition_id}, offset_int={offset_int}"
            )
        except (ValueError, TypeError) as e:
            logger.error(
                f"❌ CRITICAL: Invalid offset format: offset={offset!r}, type={type(offset).__name__}, "
                f"error={e}, using sequence_number={sequence_number} as fallback"
            )
            offset_int = sequence_number

        # Save OFFSET to MotherDuck (stored in waterlevel column)
        # Also save sequence_number and other info in metadata JSON
        partition_checkpoints = {partition_id: offset_int}
        partition_metadata = {
            partition_id: {
                "sequence_number": sequence_number,
                "offset_string": offset,  # Keep original string format
                "fully_qualified_namespace": checkpoint.get(
                    "fully_qualified_namespace"
                ),
                "eventhub_name": checkpoint.get("eventhub_name"),
                "consumer_group": checkpoint.get("consumer_group"),
            }
        }

        logger.info(
            f"💾 Calling save_checkpoint: partition={partition_id}, "
            f"waterlevel={offset_int}, metadata.sequence_number={sequence_number}"
        )

        success = await self.checkpoint_manager.save_checkpoint(
            partition_checkpoints, partition_metadata
        )

        if success:
            logger.debug(
                f"Checkpoint updated for partition {partition_id}: offset={offset}, sequence={sequence_number}"
            )
        else:
            logger.warning(f"Failed to update checkpoint for partition {partition_id}")

    async def list_checkpoints(
        self, fully_qualified_namespace: str, eventhub_name: str, consumer_group: str
    ) -> List[Dict[str, Any]]:
        """
        List all checkpoints.

        Returns:
            List of checkpoint dictionaries with keys:
                - fully_qualified_namespace
                - eventhub_name
                - consumer_group
                - partition_id
                - offset
                - sequence_number
        """
        # Load checkpoints from MotherDuck
        checkpoints_data = await self.checkpoint_manager.get_last_checkpoint()

        if not checkpoints_data:
            return []

        # Convert MotherDuck format to Azure SDK format
        # CRITICAL: waterlevel column stores the OFFSET (not sequence number)
        # The SDK needs the offset to resume from the correct position
        checkpoints = []
        for partition_id, offset_value in checkpoints_data.items():
            checkpoint = {
                "fully_qualified_namespace": fully_qualified_namespace,
                "eventhub_name": eventhub_name,
                "consumer_group": consumer_group,
                "partition_id": partition_id,
                "offset": str(offset_value),  # SDK expects offset as string
                "sequence_number": offset_value,  # We don't have separate seq num, use offset
            }
            self._checkpoint_cache[partition_id] = checkpoint
            checkpoints.append(checkpoint)
            logger.info(
                f"📍 Returning checkpoint to SDK: partition={partition_id}, offset={offset_value}"
            )

        logger.info(f"Loaded {len(checkpoints)} checkpoints from MotherDuck for SDK")
        return checkpoints


class EventHubAsyncConsumer:
    """
    Async EventHub consumer with MotherDuck-based checkpoint management.

    This consumer:
    - Receives messages from EventHub asynchronously
    - Batches messages for efficient processing
    - Uses MotherDuck for checkpoint storage
    - Provides callback-based message processing
    """

    def __init__(
        self,
        eventhub_config: EventHubConfig,
        target_db: str,
        target_schema: str,
        target_table: str,
        message_processor: Callable[[List[EventHubMessage]], bool],
        motherduck_config: Optional[MotherDuckConnectionConfig] = None,
        batch_size: int = 1000,
        batch_timeout_seconds: int = 300,
    ):
        self.eventhub_config = eventhub_config
        self.target_db = target_db
        self.target_schema = target_schema
        self.target_table = target_table
        self.message_processor = message_processor
        self.motherduck_config = motherduck_config
        self.batch_size = batch_size
        self.batch_timeout_seconds = batch_timeout_seconds

        # Runtime state
        self.client: Optional[EventHubConsumerClient] = None
        self.checkpoint_manager: Optional[MotherDuckCheckpointManager] = None
        self.current_batch: Optional[MessageBatch] = None
        self.running = False
        self.tasks: Set[asyncio.Task] = set()
        self._first_message_logged: Set[str] = (
            set()
        )  # Track first message per partition

        # Statistics
        self.stats: Dict[str, Any] = {
            "messages_received": 0,
            "batches_processed": 0,
            "last_checkpoint": None,
            "start_time": None,
        }

    async def start(self) -> None:
        """Start the EventHub consumer."""
        if self.running:
            logger.warning("Consumer is already running")
            return

        logger.info(f"🚀 Starting EventHub consumer for {self.eventhub_config.name}")
        logger.info(f"   Namespace: {self.eventhub_config.namespace}")
        logger.info(f"   Consumer Group: {self.eventhub_config.consumer_group}")
        logger.info(f"   Batch Size: {self.batch_size}")
        logger.info(f"   Batch Timeout: {self.batch_timeout_seconds}s")

        try:
            # Initialize checkpoint manager
            logger.info("📍 Initializing checkpoint manager...")
            self.checkpoint_manager = MotherDuckCheckpointManager(
                eventhub_namespace=self.eventhub_config.namespace,
                eventhub_name=self.eventhub_config.name,
                target_db=self.target_db,
                target_schema=self.target_schema,
                target_table=self.target_table,
                motherduck_config=self.motherduck_config,
            )

            # Create Azure SDK-compatible checkpoint store
            logger.info("🔐 Creating checkpoint store...")
            checkpoint_store = MotherDuckCheckpointStore(self.checkpoint_manager)

            # Log existing checkpoints for debugging (SDK will load them automatically)
            logger.info("🔍 Checking for existing checkpoints...")
            partition_checkpoints = await self.checkpoint_manager.get_last_checkpoint()

            if partition_checkpoints:
                logger.info(
                    f"✅ Found checkpoints in MotherDuck: {partition_checkpoints}"
                )
                logger.info(
                    "   SDK will automatically resume from NEXT sequence after these checkpoints:"
                )
                for partition_id, seq_num in partition_checkpoints.items():
                    logger.info(
                        f"      Partition {partition_id}: last processed seq={seq_num}, will resume from seq={seq_num + 1}"
                    )
                self.stats["last_checkpoint"] = partition_checkpoints
            else:
                logger.warning(
                    "⚠️ No checkpoints found in MotherDuck. SDK will start from LATEST (only NEW messages will be received)."
                )
                logger.warning(
                    "   To process all messages from the beginning, you need to set starting_position='-1'"
                )

            # Create EventHub client WITH checkpoint store
            # The SDK will automatically load checkpoints from the store
            # DO NOT pass starting_position - let the SDK handle it via checkpoint_store
            logger.info("🔌 Creating EventHub client with checkpoint store...")
            self.client = EventHubConsumerClient(
                fully_qualified_namespace=self.eventhub_config.namespace,
                eventhub_name=self.eventhub_config.name,
                credential=DefaultAzureCredential(),
                consumer_group=self.eventhub_config.consumer_group,
                checkpoint_store=checkpoint_store,
            )
            logger.info(
                "✅ EventHub client created - SDK will use checkpoint store to resume"
            )

            # Initialize batch
            self.current_batch = MessageBatch(
                max_size=self.batch_size, max_wait_seconds=self.batch_timeout_seconds
            )
            logger.info(
                f"📦 Initialized message batch (max: {self.batch_size}, timeout: {self.batch_timeout_seconds}s)"
            )

            self.running = True
            self.stats["start_time"] = datetime.now(timezone.utc)

            # Start batch timeout task
            timeout_task = asyncio.create_task(self._batch_timeout_handler())
            self.tasks.add(timeout_task)
            logger.info("⏰ Batch timeout handler started")

            # Start receiving messages
            # SDK will automatically load checkpoints from checkpoint_store
            # and resume from the correct position for each partition
            logger.info("👂 Starting to receive messages from EventHub...")
            logger.info("⏳ SDK loading checkpoints from store and resuming...")
            await self.client.receive(
                on_event=self._on_event,
                # DO NOT pass starting_position - let SDK use checkpoint_store
            )

        except Exception as e:
            logger.error(f"❌ Failed to start EventHub consumer: {e}", exc_info=True)
            await self.stop()
            raise

    async def stop(self) -> None:
        """Stop the EventHub consumer gracefully."""
        if not self.running:
            return

        logger.info("🛑 Stopping EventHub consumer gracefully...")
        self.running = False

        # Close EventHub client first to stop receiving new messages
        if self.client:
            logger.info("� Closing EventHub client to stop receiving messages...")
            try:
                await self.client.close()
            except Exception as e:
                logger.warning(f"Error closing EventHub client: {e}")
            self.client = None

        # Cancel timeout handler task
        for task in self.tasks:
            if not task.done():
                task.cancel()

        # Wait for tasks to complete
        if self.tasks:
            await asyncio.gather(*self.tasks, return_exceptions=True)
        self.tasks.clear()

        # Now process any remaining messages in current batch
        if self.current_batch and self.current_batch.messages:
            message_count = len(self.current_batch.messages)
            logger.info(
                f"📦 Processing {message_count} remaining messages before shutdown..."
            )
            try:
                await self._process_batch(self.current_batch)
                logger.info(
                    f"✅ {message_count} remaining messages processed and checkpoints updated"
                )
            except Exception as e:
                logger.error(f"❌ Error processing remaining batch: {e}", exc_info=True)
        else:
            logger.info("✅ No remaining messages to process")

        # Close checkpoint manager
        if self.checkpoint_manager:
            self.checkpoint_manager.close()
            self.checkpoint_manager = None

        logger.info("✅ EventHub consumer stopped gracefully")

    async def _on_event(
        self, partition_context: PartitionContext, event: EventData | None
    ) -> None:
        """Handle incoming EventHub message."""
        if not self.running:
            logger.debug("Consumer not running, ignoring event")
            return

        if event is None:
            logger.debug(
                f"Received None event on partition {partition_context.partition_id}"
            )
            return

        try:
            # Log FIRST message received on each partition to verify checkpoint resumption
            if partition_context.partition_id not in self._first_message_logged:
                logger.warning(
                    f"🎯 FIRST MESSAGE on partition {partition_context.partition_id}: "
                    f"offset={event.offset}, sequence={event.sequence_number}, "
                    f"enqueued_time={event.enqueued_time}"
                )
                self._first_message_logged.add(partition_context.partition_id)

            logger.info(
                f"📨 Received event on partition {partition_context.partition_id}, "
                f"offset: {event.offset}, sequence: {event.sequence_number}, "
                f"enqueued_time: {event.enqueued_time}"
            )

            # Ensure sequence_number is not None
            if event.sequence_number is None:
                logger.warning(
                    f"Received event with None sequence_number on partition {partition_context.partition_id}"
                )
                return

            # Create message wrapper
            message = EventHubMessage(
                event_data=event,
                partition_id=partition_context.partition_id,
                sequence_number=event.sequence_number,
            )

            # Store partition_context with message for later checkpoint update
            message.partition_context = partition_context

            self.stats["messages_received"] += 1
            logger.info(
                f"✅ Message {self.stats['messages_received']} added to batch. "
                f"Current batch size: {len(self.current_batch.messages) if self.current_batch else 0}"
            )

            # Add to current batch
            if self.current_batch and self.current_batch.add_message(message):
                # Batch is ready - process it
                logger.info(
                    f"🔄 Batch ready for processing ({len(self.current_batch.messages)} messages)"
                )
                await self._process_batch(self.current_batch)
                self.current_batch = MessageBatch(
                    max_size=self.batch_size,
                    max_wait_seconds=self.batch_timeout_seconds,
                )

        except Exception as e:
            logger.error(f"Error processing event: {e}", exc_info=True)

    async def _batch_timeout_handler(self) -> None:
        """Handle batch timeout - process partial batches."""
        logger.info("⏰ Batch timeout handler started")
        check_count = 0
        while self.running:
            try:
                await asyncio.sleep(10)  # Check every 10 seconds
                check_count += 1

                if check_count % 6 == 0:  # Log every minute
                    logger.info(
                        f"⏰ Timeout check #{check_count}: "
                        f"Batch has {len(self.current_batch.messages) if self.current_batch else 0} messages, "
                        f"age: {time.time() - self.current_batch.created_at if self.current_batch else 0:.1f}s"
                    )

                if (
                    self.current_batch
                    and self.current_batch.messages
                    and self.current_batch.is_ready()
                ):
                    # Process timed-out batch
                    logger.info(
                        f"⏱️ Batch timeout reached! Processing {len(self.current_batch.messages)} messages"
                    )
                    await self._process_batch(self.current_batch)
                    self.current_batch = MessageBatch(
                        max_size=self.batch_size,
                        max_wait_seconds=self.batch_timeout_seconds,
                    )

            except asyncio.CancelledError:
                logger.info("⏰ Batch timeout handler cancelled")
                break
            except Exception as e:
                logger.error(f"❌ Error in batch timeout handler: {e}", exc_info=True)

    async def _process_batch(self, batch: MessageBatch) -> None:
        """Process a batch of messages."""
        if not batch.messages:
            logger.debug("No messages in batch to process")
            return

        logger.info(f"🔄 Processing batch of {len(batch.messages)} messages")
        logger.info(
            f"   Partitions in batch: {list(batch.last_sequence_by_partition.keys())}"
        )
        logger.info(f"   Sequence ranges: {batch.last_sequence_by_partition}")

        try:
            # Call the message processor
            logger.info("📤 Calling message processor...")
            success = self.message_processor(batch.messages)

            if success:
                logger.info("✅ Message processor returned success")

                # CRITICAL: Update EventHub SDK checkpoints for each partition
                # This tells EventHub where we've successfully processed up to
                # Group messages by partition to get the last message per partition
                last_message_by_partition: Dict[str, EventHubMessage] = {}
                for message in batch.messages:
                    partition_id = message.partition_id
                    if (
                        partition_id not in last_message_by_partition
                        or message.sequence_number
                        > last_message_by_partition[partition_id].sequence_number
                    ):
                        last_message_by_partition[partition_id] = message

                # Update checkpoint for each partition through the SDK
                logger.info(
                    f"🔖 Updating EventHub SDK checkpoints for {len(last_message_by_partition)} partitions..."
                )
                for partition_id, last_message in last_message_by_partition.items():
                    if last_message.partition_context:
                        try:
                            await last_message.partition_context.update_checkpoint(
                                last_message.event_data
                            )
                            logger.info(
                                f"✅ Updated SDK checkpoint for partition {partition_id}: "
                                f"offset={last_message.event_data.offset}, sequence={last_message.sequence_number}"
                            )
                        except Exception as e:
                            logger.error(
                                f"❌ Failed to update SDK checkpoint for partition {partition_id}: {e}",
                                exc_info=True,
                            )
                    else:
                        logger.warning(
                            f"⚠️ No partition_context for partition {partition_id}, cannot update SDK checkpoint"
                        )

                # NOTE: Checkpoints are already saved via SDK's CheckpointStore.update_checkpoint() above
                # No need for backup save - it would use sequence numbers instead of offsets

                self.stats["batches_processed"] += 1
                # Store checkpoint data in stats for monitoring (sequence numbers for display)
                partition_checkpoints = batch.get_checkpoint_data()
                self.stats["last_checkpoint"] = partition_checkpoints.copy()

                logger.info(
                    f"✅ Batch processed successfully! Total batches: {self.stats['batches_processed']}, "
                    f"Total messages: {self.stats['messages_received']}"
                )
            else:
                logger.error("❌ Message processor returned failure")

        except Exception as e:
            logger.error(f"❌ Error processing batch: {e}", exc_info=True)

    def get_stats(self) -> Dict[str, Any]:
        """Get consumer statistics."""
        stats = self.stats.copy()
        if stats["start_time"] is not None:
            runtime = datetime.now(timezone.utc) - stats["start_time"]
            stats["runtime_seconds"] = runtime.total_seconds()
            if stats["messages_received"] > 0:
                stats["messages_per_second"] = (
                    stats["messages_received"] / runtime.total_seconds()
                )
        return stats


async def create_eventhub_consumer(
    eventhub_config: EventHubConfig,
    target_db: str,
    target_schema: str,
    target_table: str,
    message_processor: Callable[[List[EventHubMessage]], bool],
    motherduck_config: Optional[MotherDuckConnectionConfig] = None,
    batch_size: int = 1000,
    batch_timeout_seconds: int = 300,
) -> EventHubAsyncConsumer:
    """Factory function to create an EventHub consumer."""
    return EventHubAsyncConsumer(
        eventhub_config=eventhub_config,
        target_db=target_db,
        target_schema=target_schema,
        target_table=target_table,
        message_processor=message_processor,
        motherduck_config=motherduck_config,
        batch_size=batch_size,
        batch_timeout_seconds=batch_timeout_seconds,
    )


# Example usage
if __name__ == "__main__":
    import logging

    # Configure logging
    logging.basicConfig(level=logging.INFO)

    # Example message processor
    def example_processor(messages: List[EventHubMessage]) -> bool:
        """Example message processor - just log the messages."""
        logger.info(f"Processing {len(messages)} messages")
        for msg in messages:
            logger.info(f"  Partition {msg.partition_id}: {msg.body[:100]}...")
        return True

    # Example usage would require actual EventHub configuration
    print("EventHub consumer module loaded successfully")
    print("Use create_eventhub_consumer() to create consumer instances")
