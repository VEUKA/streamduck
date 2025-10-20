# Issue: Add `__main__.py` Tests

## Priority
🟢 **LOW**

## Labels
- `testing`
- `priority-low`
- `coverage`

## Estimated Effort
1 hour

## Target Coverage Increase
+0.5%

## Description

Add test coverage for `src/__main__.py` to ensure the module can be executed correctly as a script.

## Scope

- [ ] Test `python -m streamduck` execution
- [ ] Verify it calls main CLI correctly
- [ ] Test error handling in entry point

## Implementation Example

```python
def test_main_module_execution(monkeypatch):
    """Test that python -m streamduck executes correctly."""
    from src import __main__
    import sys
    
    # Mock sys.argv
    monkeypatch.setattr(sys, 'argv', ['streamduck', 'version'])
    
    # Mock the CLI app to avoid actual execution
    with patch('src.__main__.cli_main') as mock_cli:
        # This would normally call cli_main()
        __main__.cli_main()
        
        mock_cli.assert_called_once()

def test_main_module_import():
    """Test that __main__ module can be imported."""
    from src import __main__
    
    assert __main__ is not None
```

## Acceptance Criteria

- [ ] Module execution tested
- [ ] Entry point verified
- [ ] Tests pass in CI/CD
- [ ] Coverage for `__main__.py` reaches 100%

## Related Issues

- Issue #10: Code Coverage is Low (parent issue)

## References

- [Python __main__ Documentation](https://docs.python.org/3/library/__main__.html)
