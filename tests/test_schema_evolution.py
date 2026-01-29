"""Tests for schema evolution and migration."""
import pytest
from datetime import datetime

from wiki_pipeline.schema import (
    SCHEMA_FIELDS,
    SCHEMA_VERSION,
    BQ_SCHEMA,
    DLQ_SCHEMA,
    AI_SCHEMA,
    SchemaValidator,
    validate_message_structure,
    ValidationError,
)


class TestSchemaVersioning:
    """Tests for schema version tracking."""

    def test_schema_version_is_defined(self):
        """Schema version should be defined."""
        assert SCHEMA_VERSION is not None
        assert isinstance(SCHEMA_VERSION, str)
        assert len(SCHEMA_VERSION) > 0

    def test_schema_version_format(self):
        """Schema version should follow semantic versioning."""
        parts = SCHEMA_VERSION.split('.')
        assert len(parts) == 3, "Version should be X.Y.Z format"
        assert all(part.isdigit() for part in parts), "Version parts should be numeric"

    def test_validator_uses_current_version(self):
        """SchemaValidator should use current schema version."""
        validator = SchemaValidator()
        assert validator.schema_version == SCHEMA_VERSION

    def test_dlq_records_include_schema_version(self):
        """Dead letter records should track schema version."""
        validator = SchemaValidator()
        
        invalid_record = {'title': 'Test'}
        errors = [ValidationError(
            category='MISSING_REQUIRED_FIELD',
            field='evt_id',
            expected_type='STRING',
            actual_value=None,
            message='Missing field'
        )]
        
        dlq_record = validator.create_dlq_record(invalid_record, errors)
        
        assert '_schema_version' in dlq_record
        assert dlq_record['_schema_version'] == SCHEMA_VERSION


class TestSchemaFieldDefinitions:
    """Tests for schema field definitions."""

    def test_all_required_fields_defined(self):
        """All required fields should be in SCHEMA_FIELDS."""
        required_fields = ['evt_id', 'type', 'domain']
        
        for field in required_fields:
            assert field in SCHEMA_FIELDS, f"Required field {field} not in SCHEMA_FIELDS"
            assert SCHEMA_FIELDS[field]['required'] is True

    def test_schema_fields_have_type(self):
        """All schema fields should have a type defined."""
        for field_name, config in SCHEMA_FIELDS.items():
            assert 'type' in config, f"Field {field_name} missing type"
            assert config['type'] in ['STRING', 'INT64', 'BOOL', 'TIMESTAMP'], \
                f"Field {field_name} has invalid type: {config['type']}"

    def test_schema_fields_have_required_flag(self):
        """All schema fields should specify if required."""
        for field_name, config in SCHEMA_FIELDS.items():
            assert 'required' in config, f"Field {field_name} missing required flag"
            assert isinstance(config['required'], bool)

    def test_schema_fields_have_nullable_flag(self):
        """All schema fields should specify if nullable."""
        for field_name, config in SCHEMA_FIELDS.items():
            assert 'nullable' in config, f"Field {field_name} missing nullable flag"
            assert isinstance(config['nullable'], bool)

    def test_bq_schema_string_format(self):
        """BigQuery schema should be properly formatted."""
        assert isinstance(BQ_SCHEMA, str)
        assert len(BQ_SCHEMA.strip()) > 0
        
        # Should contain field definitions
        lines = [line.strip() for line in BQ_SCHEMA.strip().split('\n') if line.strip()]
        assert len(lines) > 0
        
        # Each line should have field:type format
        for line in lines:
            assert ':' in line, f"Schema line missing colon: {line}"

    def test_dlq_schema_includes_metadata_fields(self):
        """DLQ schema should include validation metadata fields."""
        required_metadata = [
            '_validation_timestamp',
            '_error_category',
            '_error_details',
            '_original_payload',
            '_schema_version'
        ]
        
        for field in required_metadata:
            assert field in DLQ_SCHEMA, f"DLQ schema missing {field}"

    def test_ai_schema_includes_tracking_fields(self):
        """AI events schema should include tracking fields."""
        required_fields = [
            'event_id',
            'rule_version',
            'detection_timestamp',
            'ai_flags',
            'content_sample',
            'detection_metadata'
        ]
        
        for field in required_fields:
            assert field in AI_SCHEMA, f"AI schema missing {field}"


class TestSchemaBackwardCompatibility:
    """Tests for backward compatibility with old schema versions."""

    @pytest.fixture
    def validator(self):
        """Create schema validator."""
        return SchemaValidator()

    def test_validates_minimal_required_fields(self, validator):
        """Should validate record with only required fields."""
        minimal_record = {
            'evt_id': 'test-123',
            'type': 'edit',
            'domain': 'test.org'
        }
        
        is_valid, errors = validator.validate_record(minimal_record)
        assert is_valid is True
        assert len(errors) == 0

    def test_accepts_extra_fields_gracefully(self, validator):
        """Should accept records with extra fields (forward compatibility)."""
        record_with_extra = {
            'evt_id': 'test-123',
            'type': 'edit',
            'domain': 'test.org',
            'future_field_v2': 'new data',
            'another_new_field': 123
        }
        
        is_valid, errors = validator.validate_record(record_with_extra)
        # Should still validate core fields
        assert is_valid is True

    def test_handles_missing_optional_fields(self, validator):
        """Should handle records missing optional fields."""
        record_missing_optional = {
            'evt_id': 'test-123',
            'type': 'edit',
            'domain': 'test.org'
            # Missing all optional fields
        }
        
        is_valid, errors = validator.validate_record(record_missing_optional)
        assert is_valid is True

    def test_validates_different_event_types(self, validator):
        """Should validate all event types correctly."""
        event_types = ['edit', 'new', 'log', 'categorize', 'move']
        
        for event_type in event_types:
            record = {
                'evt_id': f'test-{event_type}',
                'type': event_type,
                'domain': 'test.org'
            }
            
            is_valid, errors = validator.validate_record(record)
            assert is_valid is True, f"Failed to validate {event_type} event"


class TestSchemaMigration:
    """Tests for schema migration scenarios."""

    @pytest.fixture
    def validator(self):
        """Create schema validator."""
        return SchemaValidator()

    def test_migration_adds_default_values(self, validator):
        """Migration should add default values for new fields."""
        old_record = {
            'evt_id': 'test-123',
            'type': 'edit',
            'domain': 'test.org'
        }
        
        # Simulate validation filling in defaults
        is_valid, errors = validator.validate_record(old_record)
        
        # Even if validation fails, DLQ record should have defaults
        if not is_valid:
            dlq_record = validator.create_dlq_record(old_record, errors)
            
            # Check that defaults were added
            assert 'title' in dlq_record
            assert 'user' in dlq_record
            assert 'bot' in dlq_record

    def test_handles_type_changes_gracefully(self, validator):
        """Should handle type mismatches with clear errors."""
        record_with_wrong_types = {
            'evt_id': 'test-123',
            'type': 'edit',
            'domain': 'test.org',
            'ns': 'should_be_int',  # Wrong type
            'bot': 'should_be_bool'  # Wrong type
        }
        
        is_valid, errors = validator.validate_record(record_with_wrong_types)
        
        assert is_valid is False
        assert len(errors) > 0
        
        # Errors should be categorized
        type_errors = [e for e in errors if e.category == 'TYPE_MISMATCH']
        assert len(type_errors) > 0

    def test_dlq_preserves_original_data(self, validator):
        """DLQ should preserve original data for debugging."""
        original_record = {
            'evt_id': 'test-123',
            'type': 'edit',
            'domain': 'test.org',
            'custom_field': 'custom_value'
        }
        
        errors = [ValidationError(
            category='TEST',
            field='test',
            expected_type='STRING',
            actual_value='test',
            message='Test error'
        )]
        
        dlq_record = validator.create_dlq_record(original_record, errors)
        
        assert '_original_payload' in dlq_record
        assert 'custom_field' in str(dlq_record['_original_payload'])

    def test_validation_timestamp_added(self, validator):
        """DLQ records should include validation timestamp."""
        record = {'evt_id': 'test'}
        errors = [ValidationError('TEST', 'test', 'STRING', 'test', 'Test')]
        
        dlq_record = validator.create_dlq_record(record, errors)
        
        assert '_validation_timestamp' in dlq_record
        assert isinstance(dlq_record['_validation_timestamp'], datetime)


class TestSchemaEvolutionPatterns:
    """Tests for common schema evolution patterns."""

    def test_adding_optional_field_is_safe(self):
        """Adding optional fields should not break validation."""
        # Simulate old record without new optional field
        old_record = {
            'evt_id': 'test-123',
            'type': 'edit',
            'domain': 'test.org'
        }
        
        validator = SchemaValidator()
        is_valid, errors = validator.validate_record(old_record)
        
        # Should still validate
        assert is_valid is True

    def test_required_field_validation_strict(self):
        """Required fields should be strictly enforced."""
        validator = SchemaValidator()
        
        # Missing required field
        incomplete_record = {
            'type': 'edit',
            'domain': 'test.org'
            # Missing evt_id
        }
        
        is_valid, errors = validator.validate_record(incomplete_record)
        
        assert is_valid is False
        missing_errors = [e for e in errors if e.category == 'MISSING_REQUIRED_FIELD']
        assert len(missing_errors) > 0

    def test_message_structure_validation(self):
        """Message structure validation should catch malformed messages."""
        # Valid structure
        valid_msg = {
            'meta': {
                'id': 'test-123',
                'dt': '2023-01-01T12:00:00Z',
                'domain': 'test.org'
            },
            'type': 'edit'
        }
        assert validate_message_structure(valid_msg) is True
        
        # Invalid structure - missing meta
        invalid_msg = {'type': 'edit'}
        assert validate_message_structure(invalid_msg) is False
        
        # Invalid structure - meta not dict
        invalid_msg2 = {'meta': 'string', 'type': 'edit'}
        assert validate_message_structure(invalid_msg2) is False
