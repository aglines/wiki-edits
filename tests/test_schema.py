"""Unit tests for schema validation."""
import pytest
from datetime import datetime

from wiki_pipeline.schema import (
    validate_message_structure,
    SchemaValidator,
    ValidationError,
    VALID_EVENT_TYPES,
    REQUIRED_MESSAGE_FIELDS,
)


class TestValidateMessageStructure:
    """Tests for validate_message_structure function."""

    def test_valid_edit_message(self):
        """Valid edit message should pass validation."""
        msg = {
            'meta': {
                'id': 'test-123',
                'dt': '2023-01-01T12:00:00Z',
                'domain': 'en.wikipedia.org'
            },
            'type': 'edit',
            'title': 'Test'
        }
        assert validate_message_structure(msg) is True

    def test_valid_log_message(self):
        """Valid log message should pass validation."""
        msg = {
            'meta': {
                'id': 'log-456',
                'dt': '2023-01-01T12:00:00Z',
                'domain': 'en.wikipedia.org'
            },
            'type': 'log'
        }
        assert validate_message_structure(msg) is True

    def test_missing_meta(self):
        """Message without meta should fail."""
        msg = {'type': 'edit', 'title': 'Test'}
        assert validate_message_structure(msg) is False

    def test_meta_not_dict(self):
        """Message with non-dict meta should fail."""
        msg = {'meta': 'invalid', 'type': 'edit'}
        assert validate_message_structure(msg) is False

    def test_missing_meta_id(self):
        """Message without meta.id should fail."""
        msg = {
            'meta': {
                'dt': '2023-01-01T12:00:00Z',
                'domain': 'en.wikipedia.org'
            },
            'type': 'edit'
        }
        assert validate_message_structure(msg) is False

    def test_missing_meta_dt(self):
        """Message without meta.dt should fail."""
        msg = {
            'meta': {
                'id': 'test-123',
                'domain': 'en.wikipedia.org'
            },
            'type': 'edit'
        }
        assert validate_message_structure(msg) is False

    def test_missing_meta_domain(self):
        """Message without meta.domain should fail."""
        msg = {
            'meta': {
                'id': 'test-123',
                'dt': '2023-01-01T12:00:00Z'
            },
            'type': 'edit'
        }
        assert validate_message_structure(msg) is False

    def test_invalid_event_type(self):
        """Message with invalid event type should fail."""
        msg = {
            'meta': {
                'id': 'test-123',
                'dt': '2023-01-01T12:00:00Z',
                'domain': 'en.wikipedia.org'
            },
            'type': 'invalid_type'
        }
        assert validate_message_structure(msg) is False

    def test_missing_type(self):
        """Message without type should fail."""
        msg = {
            'meta': {
                'id': 'test-123',
                'dt': '2023-01-01T12:00:00Z',
                'domain': 'en.wikipedia.org'
            }
        }
        assert validate_message_structure(msg) is False

    @pytest.mark.parametrize("event_type", list(VALID_EVENT_TYPES))
    def test_all_valid_event_types(self, event_type):
        """All valid event types should pass."""
        msg = {
            'meta': {
                'id': 'test-123',
                'dt': '2023-01-01T12:00:00Z',
                'domain': 'en.wikipedia.org'
            },
            'type': event_type
        }
        assert validate_message_structure(msg) is True


class TestSchemaValidator:
    """Tests for SchemaValidator class."""

    @pytest.fixture
    def validator(self):
        """Create validator instance."""
        return SchemaValidator()

    @pytest.fixture
    def valid_record(self):
        """Valid record with all required fields."""
        return {
            'evt_id': 'test-123',
            'type': 'edit',
            'domain': 'en.wikipedia.org',
            'title': 'Test Article',
            'user': 'TestUser',
            'bot': False,
            'ns': 0,
            'minor': True,
            'patrolled': False,
            'len_old': 100,
            'len_new': 150
        }

    def test_validate_valid_record(self, validator, valid_record):
        """Valid record should pass validation."""
        is_valid, errors = validator.validate_record(valid_record)
        assert is_valid is True
        assert len(errors) == 0

    def test_validate_missing_required_field(self, validator):
        """Missing required field should fail."""
        record = {
            'type': 'edit',
            'domain': 'en.wikipedia.org'
        }
        is_valid, errors = validator.validate_record(record)
        assert is_valid is False
        assert len(errors) > 0
        assert any(e.category == 'MISSING_REQUIRED_FIELD' for e in errors)

    def test_validate_type_mismatch_string(self, validator, valid_record):
        """Wrong type for string field should fail."""
        valid_record['title'] = 123
        is_valid, errors = validator.validate_record(valid_record)
        assert is_valid is False
        assert any(e.category == 'TYPE_MISMATCH' and e.field == 'title' for e in errors)

    def test_validate_type_mismatch_int(self, validator, valid_record):
        """Wrong type for int field should fail."""
        valid_record['ns'] = 'not_an_int'
        is_valid, errors = validator.validate_record(valid_record)
        assert is_valid is False
        assert any(e.category == 'TYPE_MISMATCH' and e.field == 'ns' for e in errors)

    def test_validate_type_mismatch_bool(self, validator, valid_record):
        """Wrong type for bool field should fail."""
        valid_record['bot'] = 'not_a_bool'
        is_valid, errors = validator.validate_record(valid_record)
        assert is_valid is False
        assert any(e.category == 'TYPE_MISMATCH' and e.field == 'bot' for e in errors)

    def test_validate_nullable_field(self, validator, valid_record):
        """Nullable fields can be None."""
        valid_record['title'] = None
        is_valid, errors = validator.validate_record(valid_record)
        assert is_valid is True

    def test_create_dlq_record(self, validator, valid_record):
        """DLQ record should be created with metadata."""
        errors = [
            ValidationError(
                category='TYPE_MISMATCH',
                field='ns',
                expected_type='INT64',
                actual_value='invalid',
                message='Type mismatch'
            )
        ]
        dlq_record = validator.create_dlq_record(valid_record, errors)
        
        assert '_validation_timestamp' in dlq_record
        assert '_error_category' in dlq_record
        assert '_error_details' in dlq_record
        assert '_original_payload' in dlq_record
        assert '_schema_version' in dlq_record
        assert dlq_record['_error_category'] == 'TYPE_MISMATCH'

    def test_create_dlq_record_fills_missing_fields(self, validator):
        """DLQ record should fill missing required fields with defaults."""
        incomplete_record = {'title': 'Test'}
        errors = [
            ValidationError(
                category='MISSING_REQUIRED_FIELD',
                field='evt_id',
                expected_type='STRING',
                actual_value=None,
                message='Missing field'
            )
        ]
        dlq_record = validator.create_dlq_record(incomplete_record, errors)
        
        assert 'evt_id' in dlq_record
        assert dlq_record['evt_id'] == ''
        assert 'type' in dlq_record
        assert 'domain' in dlq_record

    def test_categorize_errors_missing_required(self, validator):
        """Error categorization should prioritize missing required fields."""
        errors = [
            ValidationError('MISSING_REQUIRED_FIELD', 'evt_id', 'STRING', None, 'Missing'),
            ValidationError('TYPE_MISMATCH', 'ns', 'INT64', 'invalid', 'Wrong type')
        ]
        category = validator._categorize_errors(errors)
        assert category == 'MISSING_REQUIRED_FIELD'

    def test_categorize_errors_type_mismatch(self, validator):
        """Error categorization should identify type mismatches."""
        errors = [
            ValidationError('TYPE_MISMATCH', 'ns', 'INT64', 'invalid', 'Wrong type')
        ]
        category = validator._categorize_errors(errors)
        assert category == 'TYPE_MISMATCH'

    def test_categorize_errors_valid(self, validator):
        """Empty errors should return VALID."""
        category = validator._categorize_errors([])
        assert category == 'VALID'


class TestValidationError:
    """Tests for ValidationError dataclass."""

    def test_to_dict(self):
        """ValidationError should serialize to dict."""
        error = ValidationError(
            category='TYPE_MISMATCH',
            field='ns',
            expected_type='INT64',
            actual_value=123.45,
            message='Expected integer'
        )
        result = error.to_dict()
        
        assert result['category'] == 'TYPE_MISMATCH'
        assert result['field'] == 'ns'
        assert result['expected_type'] == 'INT64'
        assert result['actual_value'] == '123.45'
        assert result['message'] == 'Expected integer'
