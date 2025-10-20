# Issue: Enhance Error Handling Coverage

## Priority
🔴 **HIGH**

## Labels
- `testing`
- `priority-high`
- `coverage`
- `reliability`

## Estimated Effort
6-8 hours

## Target Coverage Increase
+5-7%

## Description

While the existing test suite covers happy path scenarios well, error handling paths across multiple modules need enhanced coverage. Testing error scenarios is critical for building a robust and reliable streaming pipeline.

## Scope

Add comprehensive error handling test coverage across all major modules:

### Connection Failures
- [ ] Azure Event Hub connection failures
- [ ] MotherDuck connection failures
- [ ] Network timeouts during operations
- [ ] Connection pool exhaustion

### Authentication Errors
- [ ] Invalid Azure credentials
- [ ] Expired MotherDuck tokens
- [ ] Missing environment variables
- [ ] Permission denied scenarios

### Data Errors
- [ ] Invalid message formats
- [ ] Malformed JSON payloads
- [ ] Schema validation failures
- [ ] Data type mismatches

### Resource Errors
- [ ] Memory exhaustion scenarios
- [ ] Disk space issues
- [ ] Database connection limits
- [ ] Message queue overflow

### Recovery Scenarios
- [ ] Retry logic validation
- [ ] Exponential backoff verification
- [ ] Circuit breaker patterns
- [ ] Graceful degradation

## Implementation Approach

### 1. Event Hub Consumer Errors (`tests/unit/test_eventhub_consumer.py`)

```python
@pytest.mark.asyncio
async def test_consumer_connection_failure(mock_eventhub_client):
    """Test Event Hub consumer handles connection failures gracefully."""
    mock_eventhub_client.create_consumer.side_effect = ConnectionError("Connection refused")
    
    consumer = EventHubConsumer(config=mock_config)
    
    with pytest.raises(ConnectionError):
        await consumer.connect()
    
    # Verify retry logic was attempted
    assert mock_eventhub_client.create_consumer.call_count == 3

@pytest.mark.asyncio
async def test_consumer_authentication_failure(mock_eventhub_client):
    """Test Event Hub consumer handles authentication failures."""
    from azure.core.exceptions import ClientAuthenticationError
    
    mock_eventhub_client.create_consumer.side_effect = ClientAuthenticationError("Invalid credentials")
    
    consumer = EventHubConsumer(config=mock_config)
    
    with pytest.raises(ClientAuthenticationError):
        await consumer.connect()
```

### 2. MotherDuck Connection Errors (`tests/unit/test_motherduck_streaming.py`)

```python
def test_motherduck_connection_failure(mock_duckdb):
    """Test MotherDuck writer handles connection failures."""
    mock_duckdb.connect.side_effect = Exception("Connection timeout")
    
    with pytest.raises(Exception) as exc_info:
        writer = MotherDuckWriter(config=mock_config)
        writer.connect()
    
    assert "Connection timeout" in str(exc_info.value)

def test_motherduck_invalid_token(mock_duckdb):
    """Test MotherDuck writer handles invalid authentication tokens."""
    mock_duckdb.connect.side_effect = Exception("Invalid token")
    
    with pytest.raises(Exception) as exc_info:
        writer = MotherDuckWriter(config=mock_config)
        writer.connect()
    
    assert "Invalid token" in str(exc_info.value)
```

### 3. Configuration Validation Errors (`tests/unit/test_config.py`)

```python
def test_config_missing_required_field():
    """Test configuration validation with missing required fields."""
    with pytest.raises(ValidationError) as exc_info:
        config = StreamDuckConfig(
            # Missing required fields
            eventhub_namespace=""
        )
    
    assert "required" in str(exc_info.value).lower()

def test_config_invalid_type():
    """Test configuration validation with invalid data types."""
    with pytest.raises(ValidationError):
        config = StreamDuckConfig(
            batch_size="not_a_number",  # Should be int
            **valid_config_base
        )
```

### 4. Message Processing Errors (`tests/unit/test_message_batch.py`)

```python
@pytest.mark.asyncio
async def test_message_batch_invalid_json():
    """Test message batch handles invalid JSON gracefully."""
    batch = MessageBatch()
    
    invalid_message = MagicMock()
    invalid_message.body_as_json.side_effect = json.JSONDecodeError("Invalid JSON", "", 0)
    
    # Should skip invalid message and log error
    await batch.add_message(invalid_message)
    
    assert batch.size() == 0

@pytest.mark.asyncio
async def test_message_batch_schema_validation_failure():
    """Test message batch validates schema and rejects invalid messages."""
    batch = MessageBatch(schema=expected_schema)
    
    invalid_message = create_message({"wrong_field": "value"})
    
    with pytest.raises(ValidationError):
        await batch.add_message(invalid_message)
```

### 5. Orchestrator Error Handling (`tests/unit/test_orchestrator.py`)

```python
@pytest.mark.asyncio
async def test_orchestrator_consumer_failure(mock_orchestrator):
    """Test orchestrator handles consumer failures gracefully."""
    mock_orchestrator.consumers[0].start.side_effect = Exception("Consumer failed")
    
    with pytest.raises(Exception):
        await mock_orchestrator.start()
    
    # Verify cleanup was performed
    assert mock_orchestrator.cleanup_called

@pytest.mark.asyncio
async def test_orchestrator_partial_failure(mock_orchestrator):
    """Test orchestrator continues with remaining consumers if one fails."""
    mock_orchestrator.consumers[0].start.side_effect = Exception("Consumer 1 failed")
    
    # Should start remaining consumers
    await mock_orchestrator.start(fail_fast=False)
    
    assert mock_orchestrator.consumers[1].start.called
```

## Acceptance Criteria

- [ ] All connection failure scenarios are tested
- [ ] All authentication error scenarios are tested
- [ ] Invalid data handling is tested
- [ ] Resource exhaustion scenarios are tested
- [ ] Retry logic is validated
- [ ] Error messages are clear and informative
- [ ] Logging of errors is verified
- [ ] Graceful degradation is tested
- [ ] Tests pass consistently in CI/CD
- [ ] Error path coverage increases by 5-7%
- [ ] All tests are documented with clear docstrings

## Testing Checklist by Module

### `src/consumers/eventhub.py`
- [ ] Connection timeout errors
- [ ] Authentication failures
- [ ] Network errors during consumption
- [ ] Checkpoint save failures
- [ ] Message processing errors
- [ ] Retry logic validation

### `src/streaming/motherduck.py`
- [ ] Connection failures
- [ ] Invalid token errors
- [ ] Write failures
- [ ] Transaction rollback scenarios
- [ ] Schema validation errors

### `src/utils/config.py`
- [ ] Missing required fields
- [ ] Invalid data types
- [ ] Out-of-range values
- [ ] Environment variable loading errors

### `src/pipeline/orchestrator.py`
- [ ] Consumer startup failures
- [ ] Partial failure scenarios
- [ ] Shutdown error handling
- [ ] Resource cleanup on errors

## Dependencies

- `pytest`
- `pytest-mock`
- `pytest-asyncio`

## Related Issues

- Issue #10: Code Coverage is Low (parent issue)
- Future: Add error recovery strategies
- Future: Implement circuit breaker patterns

## Additional Context

### Why Error Testing Matters

1. **Reliability**: Ensures the pipeline handles failures gracefully
2. **Debugging**: Better error messages help diagnose issues quickly
3. **Production Readiness**: Tests real-world failure scenarios
4. **Confidence**: Increases confidence in deployment to production

### Common Error Testing Patterns

1. **Use pytest.raises()**: For expected exceptions
2. **Mock side_effect**: To simulate failures
3. **Verify retry logic**: Ensure retries happen as expected
4. **Check error messages**: Validate user-facing error text
5. **Test cleanup**: Verify resources are released on errors

### Example Error Test Pattern

```python
@pytest.mark.asyncio
async def test_operation_with_retry():
    """Test operation retries on transient failures."""
    mock_operation = AsyncMock()
    mock_operation.side_effect = [
        Exception("Transient error"),  # First attempt fails
        Exception("Transient error"),  # Second attempt fails
        {"success": True}              # Third attempt succeeds
    ]
    
    result = await retry_operation(mock_operation, max_retries=3)
    
    assert result == {"success": True}
    assert mock_operation.call_count == 3
```

## References

- [pytest Exception Testing](https://docs.pytest.org/en/stable/how-to/assert.html#assertions-about-expected-exceptions)
- [Azure SDK Error Handling](https://learn.microsoft.com/en-us/python/api/azure-core/azure.core.exceptions)
- Existing error tests: Search for `pytest.raises` in test files
