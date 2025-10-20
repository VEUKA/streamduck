# Issue: Enhance Integration Test Coverage

## Priority
🟡 **MEDIUM**

## Labels
- `testing`
- `priority-medium`
- `coverage`
- `integration`

## Estimated Effort
6-8 hours

## Target Coverage Increase
+3-4%

## Description

Expand integration test coverage to include more complex end-to-end scenarios, multi-topic processing, error recovery, and graceful shutdown.

## Scope

### Multi-Topic Scenarios
- [ ] Multiple Event Hub topics processed simultaneously
- [ ] Different schemas per topic
- [ ] Independent checkpointing per topic
- [ ] Topic-specific error handling

### Checkpoint Management
- [ ] Checkpoint persistence across restarts
- [ ] Checkpoint recovery after failures
- [ ] Checkpoint conflicts
- [ ] Checkpoint rollback scenarios

### Graceful Shutdown
- [ ] Clean shutdown with pending messages
- [ ] Checkpoint save on shutdown
- [ ] Resource cleanup verification
- [ ] Signal handling (SIGTERM, SIGINT)

### Error Recovery
- [ ] Recovery from transient failures
- [ ] Partial failure scenarios
- [ ] Retry and backoff behavior
- [ ] Dead letter queue handling

### Performance Scenarios
- [ ] High message volume
- [ ] Concurrent partition processing
- [ ] Backpressure handling
- [ ] Resource utilization monitoring

## Implementation Examples

### Example 1: Multi-Topic Processing
```python
@pytest.mark.asyncio
@pytest.mark.integration
async def test_multi_topic_processing():
    """Test processing multiple Event Hub topics simultaneously."""
    config = create_test_config(topics=["topic1", "topic2", "topic3"])
    orchestrator = Orchestrator(config)
    
    # Start processing
    await orchestrator.start()
    
    # Verify all topics are being processed
    assert len(orchestrator.consumers) == 3
    assert all(c.is_running() for c in orchestrator.consumers)
    
    # Stop and verify cleanup
    await orchestrator.stop()
    assert all(not c.is_running() for c in orchestrator.consumers)
```

### Example 2: Checkpoint Recovery
```python
@pytest.mark.asyncio
@pytest.mark.integration
async def test_checkpoint_recovery():
    """Test pipeline resumes from last checkpoint after restart."""
    # First run: process some messages
    orchestrator1 = Orchestrator(config)
    await orchestrator1.start()
    await orchestrator1.process_messages(count=100)
    last_checkpoint = orchestrator1.get_checkpoint()
    await orchestrator1.stop()
    
    # Second run: should resume from checkpoint
    orchestrator2 = Orchestrator(config)
    await orchestrator2.start()
    
    assert orchestrator2.get_checkpoint() == last_checkpoint
    # Should not reprocess old messages
    assert orchestrator2.processed_count == 100
```

### Example 3: Graceful Shutdown
```python
@pytest.mark.asyncio
@pytest.mark.integration
async def test_graceful_shutdown_with_pending_messages():
    """Test shutdown completes processing of pending messages."""
    orchestrator = Orchestrator(config)
    await orchestrator.start()
    
    # Add messages to queue
    await orchestrator.enqueue_messages(count=50)
    
    # Initiate shutdown
    shutdown_task = asyncio.create_task(orchestrator.stop())
    
    # Verify all messages processed before shutdown completes
    await shutdown_task
    assert orchestrator.pending_count == 0
    assert orchestrator.checkpoint_saved
```

## Acceptance Criteria

- [ ] Multi-topic scenarios tested
- [ ] Checkpoint persistence and recovery validated
- [ ] Graceful shutdown tested with pending messages
- [ ] Error recovery flows verified
- [ ] Performance under load validated
- [ ] Integration test coverage increases by 3-4%
- [ ] Tests documented with clear scenarios
- [ ] Tests pass in CI/CD pipeline

## Related Issues

- Issue #10: Code Coverage is Low (parent issue)

## References

- Existing integration tests: `tests/integration/test_pipeline_integration.py`
- [pytest-asyncio Documentation](https://pytest-asyncio.readthedocs.io/)
