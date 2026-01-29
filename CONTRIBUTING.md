# Contributing to Wikipedia Edits Pipeline

Thank you for your interest in contributing! This document provides guidelines for contributing to the project.

## Development Setup

### Prerequisites

- Python 3.9+
- [uv](https://github.com/astral-sh/uv) package manager
- Google Cloud account (for integration testing)
- Git

### Initial Setup

1. **Fork and clone the repository**

```bash
git clone https://github.com/aglines/wiki-edits.git
cd wiki-edits
```

2. **Install dependencies**

```bash
uv sync
```

3. **Set up pre-commit hooks**

```bash
pre-commit install
```

4. **Configure environment**

```bash
cp .env.example .env
# Edit .env with your GCP configuration
```

5. **Verify installation**

```bash
uv run pytest tests/
uv run python main.py local --max-events 5
```

## Development Workflow

### 1. Create a Branch

```bash
git checkout -b feature/your-feature-name
# or
git checkout -b fix/issue-number-description
```

Branch naming conventions:
- `feature/` - New features
- `fix/` - Bug fixes
- `refactor/` - Code refactoring
- `docs/` - Documentation updates
- `test/` - Test additions/updates

### 2. Make Changes

- Write clear, concise code
- Follow the code style guidelines (see below)
- Add tests for new functionality
- Update documentation as needed

### 3. Test Your Changes

```bash
# Run tests
uv run pytest tests/ -v

# Check code coverage
uv run pytest --cov=src --cov-report=html

# Run linters
uv run black src/ tests/ --check
uv run isort src/ tests/ --check
uv run flake8 src/ tests/
uv run mypy src/
```

### 4. Commit Changes

Use clear, descriptive commit messages:

```bash
git add .
git commit -m "Add feature: brief description of what changed"
```

Good commit message examples:
- `Add unit tests for AI detection patterns`
- `Fix validation error handling in MediaFilter`
- `Refactor schema definitions to reduce duplication`
- `Update README with new configuration options`

### 5. Push and Create Pull Request

```bash
git push origin your-branch-name
```

Then create a PR on GitHub with:
- Clear description of changes
- Reference to related issues
- Test results/screenshots if applicable

## Code Style Guidelines

### Python Style

We follow PEP 8 with these tools:

- **black**: Code formatting (line length: 100)
- **isort**: Import sorting
- **flake8**: Linting
- **mypy**: Type checking

### Format Your Code

```bash
# Auto-format code
uv run black src/ tests/
uv run isort src/ tests/

# Check formatting
uv run black src/ tests/ --check
uv run isort src/ tests/ --check
```

### Type Hints

Always include type hints for function parameters and return values:

```python
def process_event(event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Process a Wikipedia event.

    Args:
        event: Raw event dictionary

    Returns:
        Processed event or None if invalid
    """
    ...
```

### Docstrings

Use Google-style docstrings:

```python
def extract_fields(msg: Dict[str, Any]) -> Dict[str, Any]:
    """Extract fields from Wikipedia message.

    Args:
        msg: Raw message dictionary from Wikimedia stream

    Returns:
        Dictionary with extracted and normalized fields

    Raises:
        KeyError: If required fields are missing
        ValueError: If field values are invalid
    """
    ...
```

## Testing Guidelines

### Writing Tests

- Place unit tests in `tests/unit/`
- Place integration tests in `tests/integration/`
- Use descriptive test names: `test_extract_core_fields_with_missing_optional`
- Organize tests into classes by functionality

Example test structure:

```python
import pytest
from wiki_pipeline.transforms.extractors import extract_core_fields


class TestExtractCoreFields:
    """Test core field extraction."""

    def test_success_with_all_fields(self):
        """Test successful extraction with all fields present."""
        msg = {
            'meta': {'id': 'test-1', 'dt': '2024-01-01T00:00:00Z', 'domain': 'en.wikipedia.org'},
            'type': 'edit',
            # ... more fields
        }
        result = extract_core_fields(msg)
        assert result['evt_id'] == 'test-1'
        assert result['type'] == 'edit'

    def test_failure_with_missing_required(self):
        """Test extraction fails with missing required fields."""
        msg = {'type': 'edit'}
        with pytest.raises(KeyError):
            extract_core_fields(msg)
```

### Test Coverage

- Aim for 70%+ code coverage
- Critical paths should have 100% coverage:
  - Schema validation
  - AI detection logic
  - Field extraction

### Running Specific Tests

```bash
# Run all tests
uv run pytest

# Run specific file
uv run pytest tests/unit/test_extractors.py

# Run specific test
uv run pytest tests/unit/test_extractors.py::TestExtractCoreFields::test_success

# Run with coverage
uv run pytest --cov=src --cov-report=html
```

## Adding New Features

### Adding AI Detection Patterns

1. Edit `src/wiki_pipeline/analysis/patterns/detection_patterns.json`:

```json
{
  "version": "1.0.3",
  "patterns": {
    "new_pattern_name": {
      "type": "phrase",
      "patterns": ["phrase to detect", "another phrase"]
    }
  }
}
```

2. Add tests in `tests/unit/test_ai_detection.py`
3. Update version in `src/wiki_pipeline/analysis/patterns/detection_patterns.json`
4. Document in README

### Adding New Transforms

1. Create transform in `src/wiki_pipeline/transforms/`
2. Inherit from `beam.DoFn`
3. Add unit tests
4. Integrate into pipeline in `pipeline/processing.py`

Example:

```python
class MyTransform(beam.DoFn):
    """Description of what this transform does."""

    def process(self, element: Dict[str, Any]) -> Generator[Dict[str, Any], None, None]:
        """Process element.

        Args:
            element: Input dictionary

        Yields:
            Transformed dictionaries
        """
        # Transform logic here
        yield element
```

### Extending BigQuery Schema

1. Update schema in `src/wiki_pipeline/schema.py`
2. Add migration logic if needed
3. Update documentation
4. Test with sample data

## Pull Request Guidelines

### Before Submitting

- [ ] All tests pass locally
- [ ] Code is formatted (black, isort)
- [ ] Linters pass (flake8, mypy)
- [ ] Documentation is updated
- [ ] Commit messages are clear
- [ ] Branch is up to date with main

### PR Description Template

```markdown
## Description
Brief description of changes

## Type of Change
- [ ] Bug fix
- [ ] New feature
- [ ] Breaking change
- [ ] Documentation update

## Testing
- [ ] Unit tests added/updated
- [ ] Integration tests added/updated
- [ ] Manual testing completed

## Checklist
- [ ] Code follows style guidelines
- [ ] Self-review completed
- [ ] Documentation updated
- [ ] Tests pass
```

### Review Process

1. Automated CI checks must pass
2. At least one maintainer review required
3. Address feedback in new commits
4. Squash commits if requested before merge

## Project Structure

Understanding the codebase:

```
src/wiki_pipeline/
├── analysis/           # AI detection logic
│   ├── ai_detection.py       # Core detection functions
│   └── patterns/             # Pattern loading utilities
├── transforms/         # Apache Beam transforms
│   ├── filters.py            # Event filtering
│   ├── extractors.py         # Field extraction
│   └── validation.py         # Schema validation
└── schema.py          # BigQuery schemas

pipeline/
├── streaming.py        # Wikimedia → Pub/Sub ingestion
├── processing.py       # Dataflow pipeline definition
└── local_testing.py    # Local testing utilities

tests/
├── unit/              # Unit tests
├── integration/       # Integration tests
└── conftest.py        # Shared fixtures
```

## Common Tasks

### Adding a Dependency

```bash
# Add to pyproject.toml
uv add package-name

# Or for dev dependencies
uv add --dev package-name

# Sync environment
uv sync
```

### Updating Detection Patterns

```bash
# Edit src/wiki_pipeline/analysis/patterns/detection_patterns.json
# Bump version number
# Test locally
uv run python dev/test_ai_detection.py

# Commit with descriptive message
git commit -m "Update detection patterns: add X, remove Y"
```

### Running Local Pipeline

```bash
# Generate sample events and process
uv run python main.py local --max-events 20

# Check output
cat output/processed_events-*.jsonl | jq
```

## Getting Help

- **Bug reports**: Open an issue with reproducible example
- **Feature requests**: Open an issue with use case description
- **Questions**: Start a discussion on GitHub
- **Security issues**: Email maintainers directly (see SECURITY.md)

## Code of Conduct

- Be respectful and inclusive
- Focus on constructive feedback
- Help others learn and grow
- Follow project guidelines

## License

By contributing, you agree that your contributions will be licensed under the MIT License.
