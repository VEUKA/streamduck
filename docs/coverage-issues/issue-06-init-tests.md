# Issue: Add Package Init Tests

## Priority
🟢 **LOW**

## Labels
- `testing`
- `priority-low`
- `coverage`

## Estimated Effort
1-2 hours

## Target Coverage Increase
+1-2%

## Description

Add basic test coverage for package `__init__.py` files to ensure imports work correctly and public APIs are properly exposed.

## Scope

### Files to Test
- [ ] `src/__init__.py`
- [ ] `src/consumers/__init__.py`
- [ ] `src/pipeline/__init__.py`
- [ ] `src/streaming/__init__.py`
- [ ] `src/utils/__init__.py`

### Test Coverage
- [ ] Package imports work correctly
- [ ] `__all__` exports are valid
- [ ] Public API is accessible
- [ ] No circular import issues
- [ ] Version information accessible

## Implementation Examples

### Example 1: Test Package Imports
```python
def test_streamduck_imports():
    """Test that main package imports work correctly."""
    import src
    
    # Verify package can be imported
    assert src is not None
    
def test_consumers_imports():
    """Test that consumers package exports correct modules."""
    from src import consumers
    
    # Verify expected modules are available
    assert hasattr(consumers, 'EventHubConsumer')
```

### Example 2: Test __all__ Exports
```python
def test_utils_all_exports():
    """Test that utils package __all__ matches actual exports."""
    from src import utils
    
    if hasattr(utils, '__all__'):
        # Verify all listed exports exist
        for export in utils.__all__:
            assert hasattr(utils, export), f"{export} listed in __all__ but not found"
```

### Example 3: No Circular Imports
```python
def test_no_circular_imports():
    """Test that importing all packages doesn't cause circular import errors."""
    try:
        from src import consumers
        from src import pipeline
        from src import streaming
        from src import utils
        
        # If we get here, no circular import issues
        assert True
    except ImportError as e:
        pytest.fail(f"Circular import detected: {e}")
```

## Acceptance Criteria

- [ ] All `__init__.py` files have basic import tests
- [ ] `__all__` exports validated where present
- [ ] No circular import issues
- [ ] Tests pass in CI/CD
- [ ] Coverage for init files reaches >80%

## Related Issues

- Issue #10: Code Coverage is Low (parent issue)

## References

- Python Package documentation
- Existing test patterns in repository
