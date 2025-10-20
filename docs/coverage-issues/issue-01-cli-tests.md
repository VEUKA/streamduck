# Issue: Add CLI Tests for `src/main.py`

## Priority
🔴 **HIGH**

## Labels
- `testing`
- `priority-high`
- `coverage`
- `good-first-issue`

## Estimated Effort
4-6 hours

## Target Coverage Increase
+8-10%

## Description

The CLI module (`src/main.py` - 571 LOC) is the main entry point for the StreamDuck application but currently has no dedicated test coverage. This represents a significant gap in our test suite.

## Scope

Add comprehensive test coverage for all CLI commands and functionality:

### Commands to Test
- [ ] `run` - Main pipeline execution command
- [ ] `validate-config` - Configuration validation command  
- [ ] `status` - Pipeline status display command
- [ ] `version` - Version information command
- [ ] CLI argument parsing and validation
- [ ] Error handling and user feedback
- [ ] Dry-run mode functionality
- [ ] Logging configuration

## Implementation Approach

1. **Create Test File**: `tests/unit/test_cli.py`

2. **Use Typer Testing Utilities**:
   ```python
   from typer.testing import CliRunner
   from main import app
   
   runner = CliRunner()
   ```

3. **Mock External Dependencies**:
   - Pipeline orchestrator
   - Configuration loading
   - Azure Event Hub connections
   - MotherDuck connections
   - Logging setup

4. **Test Structure**:
   ```python
   def test_version_command():
       """Test version command displays correct version"""
       result = runner.invoke(app, ["version"])
       assert result.exit_code == 0
       assert "0.1.0" in result.output
   
   def test_validate_config_success():
       """Test validate-config with valid configuration"""
       # Mock valid config
       result = runner.invoke(app, ["validate-config"])
       assert result.exit_code == 0
       assert "Configuration valid" in result.output
   ```

## Acceptance Criteria

- [ ] All CLI commands have test coverage
- [ ] All command arguments and options are tested
- [ ] Error scenarios are tested (invalid config, connection failures, etc.)
- [ ] User-facing messages are validated
- [ ] Exit codes are verified (0 for success, 1 for errors)
- [ ] Dry-run mode is tested
- [ ] Logging configuration is tested
- [ ] Test coverage for `src/main.py` reaches >80%
- [ ] Tests pass in CI/CD pipeline
- [ ] Tests are documented with clear docstrings

## Test Examples

### Example 1: Version Command
```python
def test_version_command():
    """Test that version command displays correct version information."""
    result = runner.invoke(app, ["version"])
    assert result.exit_code == 0
    assert "streamduck" in result.output.lower()
    assert "0.1.0" in result.output
```

### Example 2: Config Validation Success
```python
@patch('main.load_config')
def test_validate_config_valid(mock_load_config):
    """Test validate-config command with valid configuration."""
    mock_load_config.return_value = MagicMock(spec=StreamDuckConfig)
    
    result = runner.invoke(app, ["validate-config"])
    
    assert result.exit_code == 0
    assert "valid" in result.output.lower()
    mock_load_config.assert_called_once()
```

### Example 3: Config Validation Failure
```python
@patch('main.load_config')
def test_validate_config_invalid(mock_load_config):
    """Test validate-config command with invalid configuration."""
    mock_load_config.side_effect = ValueError("Invalid config")
    
    result = runner.invoke(app, ["validate-config"])
    
    assert result.exit_code == 1
    assert "error" in result.output.lower()
```

### Example 4: Run Command with Dry Run
```python
@patch('main.Orchestrator')
@patch('main.load_config')
def test_run_command_dry_run(mock_load_config, mock_orchestrator):
    """Test run command in dry-run mode."""
    mock_config = MagicMock(spec=StreamDuckConfig)
    mock_load_config.return_value = mock_config
    
    result = runner.invoke(app, ["run", "--dry-run"])
    
    assert result.exit_code == 0
    assert "dry run" in result.output.lower()
    mock_orchestrator.assert_called_once()
```

## Dependencies

- `pytest`
- `pytest-mock`
- `typer` (already installed)

## Related Issues

- Issue #10: Code Coverage is Low (parent issue)
- Future: Issue for CLI argument validation enhancements

## Additional Context

The CLI is built using Typer, which provides excellent testing utilities through `typer.testing.CliRunner`. This makes it straightforward to test CLI commands in isolation by mocking external dependencies.

### Key Files
- **Source**: `src/main.py`
- **Test File**: `tests/unit/test_cli.py` (to be created)
- **Fixtures**: `tests/conftest.py` (existing fixtures can be reused)

### Testing Guidelines
1. Follow the existing test patterns in the repository
2. Use pytest fixtures for common setup
3. Mock all external dependencies (Azure, MotherDuck, file I/O)
4. Test both success and error paths
5. Validate user-facing output messages
6. Keep tests fast and independent

## References

- [Typer Testing Documentation](https://typer.tiangolo.com/tutorial/testing/)
- [pytest-mock Documentation](https://pytest-mock.readthedocs.io/)
- Existing test examples: `tests/unit/test_config.py`, `tests/unit/test_orchestrator.py`
