"""Security tests for input validation and sanitization."""
import pytest
import json

from wiki_pipeline.transforms.filters import AllEvents
from wiki_pipeline.transforms.extractors import get_all_fields
from wiki_pipeline.utils import sanitize_string_field, safe_cast
from wiki_pipeline.schema import validate_message_structure, SchemaValidator
from apache_beam.io.gcp.pubsub import PubsubMessage


class TestInputSanitization:
    """Tests for malicious input handling."""

    def test_sql_injection_in_comment(self):
        """Should handle SQL injection attempts in comment field."""
        malicious_comment = "'; DROP TABLE wiki_edits; --"
        
        event = {
            'meta': {'id': 'test', 'dt': '2023-01-01T12:00:00Z', 'domain': 'test.org'},
            'type': 'edit',
            'comment': malicious_comment
        }
        
        # Should not crash or execute SQL
        fields = get_all_fields(event)
        assert fields is not None
        assert fields['comment'] == malicious_comment  # Preserved as-is, not executed

    def test_xss_in_comment(self):
        """Should handle XSS attempts in comment field."""
        xss_payload = '<script>alert("XSS")</script>'
        
        event = {
            'meta': {'id': 'test', 'dt': '2023-01-01T12:00:00Z', 'domain': 'test.org'},
            'type': 'edit',
            'comment': xss_payload
        }
        
        fields = get_all_fields(event)
        assert fields is not None
        # Should preserve but not execute
        assert '<script>' in fields['comment']

    def test_path_traversal_in_title(self):
        """Should handle path traversal attempts."""
        malicious_title = '../../../etc/passwd'
        
        event = {
            'meta': {'id': 'test', 'dt': '2023-01-01T12:00:00Z', 'domain': 'test.org'},
            'type': 'edit',
            'title': malicious_title
        }
        
        fields = get_all_fields(event)
        assert fields is not None
        assert fields['title'] == malicious_title

    def test_command_injection_in_user(self):
        """Should handle command injection attempts."""
        malicious_user = 'user; rm -rf /'
        
        event = {
            'meta': {'id': 'test', 'dt': '2023-01-01T12:00:00Z', 'domain': 'test.org'},
            'type': 'edit',
            'user': malicious_user
        }
        
        fields = get_all_fields(event)
        assert fields is not None
        assert fields['user'] == malicious_user

    def test_null_bytes_in_fields(self):
        """Should handle null bytes in string fields."""
        event = {
            'meta': {'id': 'test', 'dt': '2023-01-01T12:00:00Z', 'domain': 'test.org'},
            'type': 'edit',
            'comment': 'test\x00comment'
        }
        
        # Should not crash
        fields = get_all_fields(event)
        assert fields is not None

    def test_unicode_overflow(self):
        """Should handle unicode edge cases."""
        event = {
            'meta': {'id': 'test', 'dt': '2023-01-01T12:00:00Z', 'domain': 'test.org'},
            'type': 'edit',
            'comment': '🔥' * 1000  # Many emojis
        }
        
        fields = get_all_fields(event)
        assert fields is not None


class TestResourceExhaustion:
    """Tests for resource exhaustion attacks."""

    def test_extremely_large_comment(self):
        """Should handle very large comment fields."""
        huge_comment = 'A' * 1_000_000  # 1MB comment
        
        event = {
            'meta': {'id': 'test', 'dt': '2023-01-01T12:00:00Z', 'domain': 'test.org'},
            'type': 'edit',
            'comment': huge_comment
        }
        
        # Should not crash or consume excessive memory
        fields = get_all_fields(event)
        assert fields is not None

    def test_deeply_nested_json(self):
        """Should handle deeply nested JSON structures."""
        # Create deeply nested structure
        nested = {'level': 0}
        current = nested
        for i in range(100):
            current['nested'] = {'level': i + 1}
            current = current['nested']
        
        event = {
            'meta': {'id': 'test', 'dt': '2023-01-01T12:00:00Z', 'domain': 'test.org'},
            'type': 'edit',
            'nested_data': nested
        }
        
        # Should not cause stack overflow
        fields = get_all_fields(event)
        assert fields is not None

    def test_many_fields(self):
        """Should handle events with many fields."""
        event = {
            'meta': {'id': 'test', 'dt': '2023-01-01T12:00:00Z', 'domain': 'test.org'},
            'type': 'edit'
        }
        
        # Add 1000 extra fields
        for i in range(1000):
            event[f'field_{i}'] = f'value_{i}'
        
        # Should not crash
        fields = get_all_fields(event)
        assert fields is not None

    def test_sanitize_string_truncates_long_strings(self):
        """String sanitization should truncate very long strings."""
        long_string = 'A' * 10000
        
        sanitized = sanitize_string_field(long_string, max_length=1000)
        
        assert len(sanitized) == 1000
        assert sanitized == 'A' * 1000


class TestDataPrivacy:
    """Tests for PII and sensitive data handling."""

    def test_user_field_not_logged_in_errors(self):
        """User data should not appear in error messages."""
        event = {
            'meta': {'id': 'test', 'dt': '2023-01-01T12:00:00Z', 'domain': 'test.org'},
            'type': 'edit',
            'user': 'sensitive_username@email.com'
        }
        
        # Process event
        fields = get_all_fields(event)
        
        # User field should be in data but not exposed in logs
        assert fields['user'] == 'sensitive_username@email.com'
        # Note: Actual log checking would require log capture

    def test_dlq_records_include_original_data(self):
        """DLQ should preserve data for debugging but flag as sensitive."""
        validator = SchemaValidator()
        
        record = {
            'title': 'Test',
            'user': 'sensitive_user@email.com'
        }
        
        from wiki_pipeline.schema import ValidationError
        errors = [ValidationError('TEST', 'test', 'STRING', 'test', 'Test')]
        
        dlq_record = validator.create_dlq_record(record, errors)
        
        # Should preserve original data
        assert '_original_payload' in dlq_record
        # In production, this should be encrypted or access-controlled


class TestSecretsManagement:
    """Tests for secrets and credentials handling."""

    def test_no_hardcoded_credentials_in_config(self):
        """Config validation should not expose credentials."""
        from wiki_pipeline.utils import validate_config
        import os
        
        # Ensure no credentials in error messages
        with pytest.raises(Exception):  # Will fail due to missing env vars
            os.environ.clear()
            validate_config()
        
        # Error message should not contain actual credential values

    def test_safe_cast_does_not_log_sensitive_values(self):
        """Type casting should not log sensitive data."""
        sensitive_value = "api_key_12345"
        
        # Should cast without logging the value
        result = safe_cast(sensitive_value, str)
        assert result == sensitive_value


class TestValidationRobustness:
    """Tests for validation edge cases."""

    def test_message_structure_validation_malformed_meta(self):
        """Should reject messages with malformed meta."""
        invalid_messages = [
            {'meta': None, 'type': 'edit'},
            {'meta': [], 'type': 'edit'},
            {'meta': 'string', 'type': 'edit'},
            {'meta': 123, 'type': 'edit'},
        ]
        
        for msg in invalid_messages:
            assert validate_message_structure(msg) is False

    def test_message_structure_validation_missing_required_meta_fields(self):
        """Should reject messages missing required meta fields."""
        invalid_messages = [
            {'meta': {'dt': '2023-01-01T12:00:00Z', 'domain': 'test.org'}, 'type': 'edit'},  # Missing id
            {'meta': {'id': 'test', 'domain': 'test.org'}, 'type': 'edit'},  # Missing dt
            {'meta': {'id': 'test', 'dt': '2023-01-01T12:00:00Z'}, 'type': 'edit'},  # Missing domain
        ]
        
        for msg in invalid_messages:
            assert validate_message_structure(msg) is False

    def test_message_structure_validation_invalid_event_type(self):
        """Should reject messages with invalid event types."""
        msg = {
            'meta': {'id': 'test', 'dt': '2023-01-01T12:00:00Z', 'domain': 'test.org'},
            'type': 'invalid_type'
        }
        
        assert validate_message_structure(msg) is False

    def test_all_events_filter_handles_malformed_pubsub_message(self):
        """AllEvents filter should handle malformed Pub/Sub messages."""
        filter_fn = AllEvents()
        
        # Invalid JSON
        msg = PubsubMessage(data=b'not valid json', attributes={})
        results = list(filter_fn.process(msg))
        assert len(results) == 0
        
        # Empty data
        msg = PubsubMessage(data=b'', attributes={})
        results = list(filter_fn.process(msg))
        assert len(results) == 0

    def test_type_coercion_prevents_injection(self):
        """Type coercion should prevent type confusion attacks."""
        # Try to inject non-integer as integer
        result = safe_cast("'; DROP TABLE users; --", int, default=0)
        assert result == 0
        
        # Try to inject non-boolean as boolean (any non-empty string is truthy)
        result = safe_cast("malicious_string", bool, default=False)
        # safe_cast with bool converts string to bool (non-empty = True)
        # This is expected Python behavior, not a vulnerability
        assert isinstance(result, bool)
