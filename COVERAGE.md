# Code Coverage Improvement - Quick Reference

## 📋 Overview

This repository contains a comprehensive plan to improve test coverage from an estimated ~60-70% to 90%+ over the next 2 months.

## 📚 Documentation

- **[Complete Plan](./docs/coverage-improvement-plan.md)** - Full strategic plan with analysis and guidelines
- **[Issue Templates](./docs/coverage-issues/)** - 7 detailed issue templates ready to be created in GitHub

## 🎯 Coverage Targets

| Timeframe | Target Coverage | Focus Areas |
|-----------|----------------|-------------|
| **Short-term** (2 weeks) | 75-80% | Phase 1: CLI tests, Error handling |
| **Mid-term** (1 month) | 85-90% | Phase 1 + 2: Add edge cases, config validation, integration |
| **Long-term** (2 months) | 90-95% | All phases complete |

## 🚀 Issues to Create

### Phase 1: Critical (HIGH Priority) - +13-17% Coverage

1. **CLI Tests** (`src/main.py`)
   - Effort: 4-6 hours | Impact: +8-10%
   - [Template](./docs/coverage-issues/issue-01-cli-tests.md)
   - Test all CLI commands, argument parsing, error handling

2. **Error Handling Coverage** (All modules)
   - Effort: 6-8 hours | Impact: +5-7%
   - [Template](./docs/coverage-issues/issue-02-error-handling.md)
   - Test connection failures, auth errors, recovery scenarios

### Phase 2: Enhanced (MEDIUM Priority) - +8-12% Coverage

3. **Edge Case Tests**
   - Effort: 4-6 hours | Impact: +3-5%
   - [Template](./docs/coverage-issues/issue-03-edge-cases.md)
   - Boundary conditions, Unicode, concurrency

4. **Configuration Validation**
   - Effort: 3-4 hours | Impact: +2-3%
   - [Template](./docs/coverage-issues/issue-04-config-validation.md)
   - Missing fields, invalid types, range validation

5. **Integration Test Enhancement**
   - Effort: 6-8 hours | Impact: +3-4%
   - [Template](./docs/coverage-issues/issue-05-integration-tests.md)
   - Multi-topic scenarios, checkpoint recovery, graceful shutdown

### Phase 3: Comprehensive (LOW Priority) - +1.5-2% Coverage

6. **Package Init Tests**
   - Effort: 1-2 hours | Impact: +1-2%
   - [Template](./docs/coverage-issues/issue-06-init-tests.md)

7. **__main__.py Tests**
   - Effort: 1 hour | Impact: +0.5%
   - [Template](./docs/coverage-issues/issue-07-main-tests.md)

## 📊 Current State

### Well-Covered Modules ✅
- `src/utils/config.py` → `tests/unit/test_config.py`
- `src/consumers/eventhub.py` → `tests/unit/test_eventhub_consumer.py`
- `src/pipeline/orchestrator.py` → `tests/unit/test_orchestrator.py`
- `src/streaming/motherduck.py` → `tests/unit/test_motherduck_streaming.py`
- `src/utils/motherduck.py` → `tests/unit/test_motherduck_utils.py`
- Message batching, Checkpoint management, Integration tests

### Coverage Gaps ❌
1. **`src/main.py`** (571 LOC) - No test coverage
2. **Error paths** - Partial coverage across modules
3. **Edge cases** - Limited boundary condition testing
4. **Init files** - Minimal test coverage

## 🛠️ Getting Started

### For Maintainers

1. **Review** the [comprehensive plan](./docs/coverage-improvement-plan.md)
2. **Create issues** using templates from [`docs/coverage-issues/`](./docs/coverage-issues/)
3. **Prioritize** Phase 1 issues first (HIGH priority)
4. **Assign** to team members
5. **Track** progress using parent issue #10

### For Contributors

1. **Pick an issue** based on priority and your expertise
2. **Read the template** - Contains implementation examples and acceptance criteria
3. **Follow patterns** - Match existing test style in the repository
4. **Use fixtures** - Leverage existing test fixtures from `tests/conftest.py`
5. **Mock dependencies** - Use pytest-mock for external services
6. **Submit PR** - Reference the issue in your pull request

## 📈 Success Metrics

- **Overall Coverage**: Target 90%+
- **Branch Coverage**: Target 85%+
- **Untested Functions**: Target <5%
- **Test Execution Time**: Keep under 2 minutes

## 🔗 Related Resources

- **Parent Issue**: [#10 - Code Coverage is Low](https://github.com/VEUKA/streamduck/issues/10)
- **CI/CD Configuration**: [`.github/workflows/tests.yml`](./.github/workflows/tests.yml)
- **pytest Configuration**: [`pyproject.toml`](./pyproject.toml) (see `[tool.pytest.ini_options]` and `[tool.coverage]`)
- **Codecov Dashboard**: [codecov.io/gh/VEUKA/streamduck](https://codecov.io/gh/VEUKA/streamduck)

## 💡 Quick Tips

### Writing Good Tests
```python
# Use descriptive test names
def test_consumer_handles_connection_timeout():
    """Test that Event Hub consumer gracefully handles connection timeouts."""
    # Arrange
    mock_client = MagicMock()
    mock_client.connect.side_effect = TimeoutError("Connection timeout")
    
    # Act & Assert
    with pytest.raises(TimeoutError):
        consumer = EventHubConsumer(client=mock_client)
        await consumer.start()
```

### Using Fixtures
```python
# Leverage existing fixtures from conftest.py
@pytest.fixture
def mock_config():
    """Provide a mock configuration for testing."""
    return MagicMock(spec=StreamDuckConfig)

def test_with_fixture(mock_config):
    """Test using fixture."""
    assert mock_config is not None
```

### Async Testing
```python
# Use pytest-asyncio for async code
@pytest.mark.asyncio
async def test_async_operation():
    """Test asynchronous operations."""
    result = await some_async_function()
    assert result.success
```

## 🤝 Contributing

All contributions to improve test coverage are welcome! Please:
1. Comment on the issue you want to work on
2. Follow the implementation guidance in the issue template
3. Ensure tests pass locally before submitting PR
4. Include test documentation (docstrings)

## 📞 Questions?

- Open a discussion on issue #10
- Review the [comprehensive plan](./docs/coverage-improvement-plan.md) for more details
- Check existing test files for patterns and examples

---

**Total Estimated Effort**: 25-35 hours  
**Total Coverage Increase**: +23-31.5%  
**Path**: ~60-70% → 90%+ coverage
