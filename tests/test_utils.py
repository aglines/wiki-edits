"""Unit tests for utility functions."""
import os
import pytest
import signal
from unittest.mock import patch, MagicMock

from wiki_pipeline.utils import (
    validate_config,
    ConfigError,
    safe_cast,
    sanitize_string_field,
    validate_required_fields,
    validate_field_types,
    ValidationError,
    TimeoutContext,
    TimeoutError,
)


class TestValidateConfig:
    """Tests for configuration validation."""

    @patch.dict(os.environ, {
        'PUBSUB_TOPIC': 'projects/test/topics/test',
        'PUBSUB_SUBSCRIPTION': 'projects/test/subscriptions/test',
        'BIGQUERY_OUTPUT_TABLE': 'project:dataset.table'
    })
    def test_valid_config(self):
        """Valid environment variables should pass."""
        config = validate_config()
        assert config['topic'] == 'projects/test/topics/test'
        assert config['subscription'] == 'projects/test/subscriptions/test'
        assert config['output_table'] == 'project:dataset.table'

    @patch.dict(os.environ, {
        'PUBSUB_TOPIC': 'projects/test/topics/test',
        'PUBSUB_SUBSCRIPTION': 'projects/test/subscriptions/test'
    }, clear=True)
    def test_missing_required_var(self):
        """Missing required variable should raise ConfigError."""
        with pytest.raises(ConfigError) as exc_info:
            validate_config()
        assert 'BIGQUERY_OUTPUT_TABLE' in str(exc_info.value)

    @patch.dict(os.environ, {}, clear=True)
    def test_all_missing(self):
        """All missing variables should raise ConfigError."""
        with pytest.raises(ConfigError) as exc_info:
            validate_config()
        assert 'PUBSUB_TOPIC' in str(exc_info.value)

    @patch.dict(os.environ, {
        'PUBSUB_TOPIC': '  ',
        'PUBSUB_SUBSCRIPTION': 'projects/test/subscriptions/test',
        'BIGQUERY_OUTPUT_TABLE': 'project:dataset.table'
    })
    def test_empty_string_treated_as_missing(self):
        """Empty/whitespace-only values should be treated as missing."""
        with pytest.raises(ConfigError):
            validate_config()


class TestSafeCast:
    """Tests for safe_cast function."""

    def test_cast_to_int(self):
        """Should cast to int successfully."""
        assert safe_cast('123', int) == 123
        assert safe_cast(123.7, int) == 123
        assert safe_cast('123.0', int) == 123

    def test_cast_to_float(self):
        """Should cast to float successfully."""
        assert safe_cast('123.45', float) == 123.45
        assert safe_cast(123, float) == 123.0

    def test_cast_to_str(self):
        """Should cast to string successfully."""
        assert safe_cast(123, str) == '123'
        assert safe_cast(True, str) == 'True'

    def test_cast_to_bool(self):
        """Should cast to bool successfully."""
        assert safe_cast('true', bool) is True
        assert safe_cast('1', bool) is True
        assert safe_cast('yes', bool) is True
        assert safe_cast('false', bool) is False
        assert safe_cast(1, bool) is True
        assert safe_cast(0, bool) is False

    def test_cast_none_returns_default(self):
        """None should return default value."""
        assert safe_cast(None, int, 42) == 42
        assert safe_cast(None, str, 'default') == 'default'

    def test_cast_invalid_returns_default(self):
        """Invalid cast should return default."""
        assert safe_cast('invalid', int, 0) == 0
        assert safe_cast('not_a_number', float, 0.0) == 0.0

    def test_cast_none_no_default(self):
        """None with no default should return None."""
        assert safe_cast(None, int) is None


class TestSanitizeStringField:
    """Tests for sanitize_string_field function."""

    def test_sanitize_normal_string(self):
        """Normal string should be returned trimmed."""
        assert sanitize_string_field('  test  ') == 'test'
        assert sanitize_string_field('hello') == 'hello'

    def test_sanitize_none(self):
        """None should return empty string."""
        assert sanitize_string_field(None) == ''

    def test_sanitize_non_string(self):
        """Non-string should be converted to string."""
        assert sanitize_string_field(123) == '123'
        assert sanitize_string_field(True) == 'True'

    def test_sanitize_truncate_long_string(self):
        """Long string should be truncated."""
        long_string = 'a' * 2000
        result = sanitize_string_field(long_string, max_length=100)
        assert len(result) == 100

    def test_sanitize_custom_max_length(self):
        """Custom max length should be respected."""
        result = sanitize_string_field('hello world', max_length=5)
        assert result == 'hello'


class TestValidateRequiredFields:
    """Tests for validate_required_fields function."""

    def test_all_fields_present(self):
        """Should pass when all required fields present."""
        data = {'field1': 'value1', 'field2': 'value2'}
        validate_required_fields(data, ['field1', 'field2'])

    def test_missing_field(self):
        """Should raise ValidationError when field missing."""
        data = {'field1': 'value1'}
        with pytest.raises(ValidationError) as exc_info:
            validate_required_fields(data, ['field1', 'field2'])
        assert 'field2' in str(exc_info.value)

    def test_multiple_missing_fields(self):
        """Should report all missing fields."""
        data = {'field1': 'value1'}
        with pytest.raises(ValidationError) as exc_info:
            validate_required_fields(data, ['field1', 'field2', 'field3'])
        assert 'field2' in str(exc_info.value)
        assert 'field3' in str(exc_info.value)


class TestValidateFieldTypes:
    """Tests for validate_field_types function."""

    def test_correct_types(self):
        """Should pass when types are correct."""
        data = {'name': 'test', 'age': 25, 'active': True}
        field_types = {'name': str, 'age': int, 'active': bool}
        validate_field_types(data, field_types)

    def test_wrong_type(self):
        """Should raise ValidationError for wrong type."""
        data = {'name': 123}
        field_types = {'name': str}
        with pytest.raises(ValidationError) as exc_info:
            validate_field_types(data, field_types)
        assert 'name' in str(exc_info.value)

    def test_missing_field_ignored(self):
        """Should not check types for missing fields."""
        data = {'name': 'test'}
        field_types = {'name': str, 'age': int}
        validate_field_types(data, field_types)


class TestTimeoutContext:
    """Tests for TimeoutContext manager."""

    def test_no_timeout_set(self):
        """Should work without timeout."""
        with TimeoutContext(hours=None) as ctx:
            assert ctx is not None

    def test_timeout_not_triggered(self):
        """Should complete normally if no timeout."""
        with TimeoutContext(hours=1):
            pass

    @patch('signal.alarm')
    @patch('signal.signal')
    def test_timeout_sets_alarm(self, mock_signal, mock_alarm):
        """Should set alarm when timeout specified."""
        with TimeoutContext(hours=1):
            pass
        mock_alarm.assert_called()

    @patch('signal.alarm')
    @patch('signal.signal')
    def test_timeout_cancels_alarm(self, mock_signal, mock_alarm):
        """Should cancel alarm on exit."""
        with TimeoutContext(hours=1):
            pass
        # Check that alarm(0) was called to cancel
        assert any(call[0][0] == 0 for call in mock_alarm.call_args_list)

    def test_timeout_calculates_seconds(self):
        """Should convert hours to seconds correctly."""
        ctx = TimeoutContext(hours=2)
        assert ctx.seconds == 7200

    def test_timeout_none_has_no_seconds(self):
        """Should have no seconds when hours is None."""
        ctx = TimeoutContext(hours=None)
        assert ctx.seconds is None
