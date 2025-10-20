# Issue: Expand Configuration Validation Tests

## Priority
🟡 **MEDIUM**

## Labels
- `testing`
- `priority-medium`
- `coverage`
- `configuration`

## Estimated Effort
3-4 hours

## Target Coverage Increase
+2-3%

## Description

Enhance test coverage for configuration validation in `src/utils/config.py` to ensure all edge cases, error scenarios, and validation logic are properly tested.

## Scope

### Missing/Invalid Values
- [ ] Missing required environment variables
- [ ] Empty string values
- [ ] None/null values
- [ ] Whitespace-only values

### Type Validation
- [ ] String instead of int
- [ ] Int instead of string
- [ ] Invalid boolean values
- [ ] Invalid enum values

### Range Validation
- [ ] Negative values where positive required
- [ ] Zero values
- [ ] Extremely large values
- [ ] Out-of-range port numbers

### Default Values
- [ ] All default values are tested
- [ ] Default value behavior
- [ ] Optional vs required fields

### Environment Variable Loading
- [ ] .env file loading
- [ ] System environment variable priority
- [ ] Variable name case sensitivity
- [ ] Variable substitution

## Implementation Examples

### Example 1: Missing Required Field
```python
def test_config_missing_eventhub_namespace():
    """Test configuration fails with missing Event Hub namespace."""
    with pytest.raises(ValidationError) as exc_info:
        config = StreamDuckConfig(
            motherduck_token="test_token",
            # eventhub_namespace is missing
        )
    
    assert "eventhub_namespace" in str(exc_info.value).lower()
    assert "required" in str(exc_info.value).lower()
```

### Example 2: Invalid Type
```python
def test_config_batch_size_invalid_type():
    """Test configuration rejects non-integer batch_size."""
    with pytest.raises(ValidationError):
        config = StreamDuckConfig(
            batch_size="one thousand",  # Should be int
            **valid_base_config
        )
```

### Example 3: Out of Range Value
```python
def test_config_batch_size_negative():
    """Test configuration rejects negative batch_size."""
    with pytest.raises(ValidationError) as exc_info:
        config = StreamDuckConfig(
            batch_size=-100,
            **valid_base_config
        )
    
    assert "positive" in str(exc_info.value).lower()
```

## Acceptance Criteria

- [ ] All required fields tested for missing values
- [ ] All type validations tested
- [ ] Range validations tested
- [ ] Default value behavior verified
- [ ] Environment variable loading tested
- [ ] Error messages are user-friendly
- [ ] Configuration coverage increases by 2-3%
- [ ] Tests documented clearly

## Related Issues

- Issue #10: Code Coverage is Low (parent issue)

## References

- [Pydantic Validation](https://docs.pydantic.dev/latest/concepts/validators/)
- Existing tests: `tests/unit/test_config.py`
