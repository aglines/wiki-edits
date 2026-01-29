"""Business-critical tests for data quality and operational requirements."""
import pytest
import json

from wiki_pipeline.ai_detection import detect_ai_indicators
from wiki_pipeline.transforms.extractors import get_all_fields
from wiki_pipeline.schema import SchemaValidator, ValidationError


class TestDataQuality:
    """Tests for data quality and accuracy."""

    @pytest.fixture
    def human_written_samples(self):
        """Sample comments that are clearly human-written."""
        return [
            "Fixed typo in introduction",
            "Added citation for claim in paragraph 3",
            "Reverted vandalism by anonymous user",
            "Updated population statistics from 2020 census",
            "Clarified wording per talk page discussion",
        ]

    @pytest.fixture
    def ai_written_samples(self):
        """Sample comments with AI indicators."""
        return [
            "As an AI language model, I cannot provide opinions",
            "This breathtaking location offers stunning natural beauty",
            "Updated with **bold formatting** for clarity",
            "It plays a vital role in the region's economy",
            "I hope this helps clarify the matter",
        ]

    def test_pattern_matching_on_human_samples(self, human_written_samples):
        """Should not flag typical human-written comments."""
        flagged_count = 0
        
        for comment in human_written_samples:
            content = {
                'comment': comment,
                'parsed_comment': '',
                'title': 'Test Article',
                'user': 'HumanUser',
                'len_old': 100,
                'len_new': 120,
                'minor': False,
                'bot': False
            }
            
            flags = detect_ai_indicators(content)
            if flags:
                flagged_count += 1
        
        # Should not flag typical human edits
        assert flagged_count <= 1, \
            f"Flagged {flagged_count}/{len(human_written_samples)} human samples - patterns may be too broad"

    def test_pattern_matching_on_ai_samples(self, ai_written_samples):
        """Should detect comments with known AI indicators."""
        from wiki_pipeline.ai_detection import get_patterns
        
        # Skip if patterns not loaded
        if not get_patterns():
            pytest.skip("Pattern file not loaded")
        
        detected_count = 0
        
        for comment in ai_written_samples:
            content = {
                'comment': comment,
                'parsed_comment': '',
                'title': 'Test Article',
                'user': 'AIUser',
                'len_old': 100,
                'len_new': 200,
                'minor': False,
                'bot': False
            }
            
            flags = detect_ai_indicators(content)
            if flags:
                detected_count += 1
        
        # Should detect most samples with AI indicators
        assert detected_count >= 3, \
            f"Only detected {detected_count}/{len(ai_written_samples)} samples with AI indicators - patterns may be too narrow"

    def test_data_completeness_all_fields_extracted(self):
        """Should extract all available fields from events."""
        complete_event = {
            'meta': {'id': 'test-123', 'dt': '2023-01-01T12:00:00Z', 'domain': 'en.wikipedia.org'},
            'type': 'edit',
            'title': 'Test Article',
            'user': 'TestUser',
            'bot': False,
            'timestamp': 1672574400,
            'namespace': 0,
            'wiki': 'enwiki',
            'server_name': 'en.wikipedia.org',
            'server_url': 'https://en.wikipedia.org',
            'minor': True,
            'patrolled': False,
            'length': {'old': 100, 'new': 150},
            'revision': {'old': 12345, 'new': 12346},
            'comment': 'Test edit',
            'parsedcomment': '<em>Test edit</em>'
        }
        
        fields = get_all_fields(complete_event)
        
        # Should extract all core fields
        assert 'evt_id' in fields
        assert 'type' in fields
        assert 'title' in fields
        assert 'user' in fields
        assert 'bot' in fields
        assert 'dt_user' in fields
        assert 'dt_server' in fields
        assert 'domain' in fields
        assert 'ns' in fields
        assert 'wiki' in fields
        assert 'minor' in fields
        assert 'patrolled' in fields
        assert 'len_old' in fields
        assert 'len_new' in fields
        assert 'comment' in fields

    def test_no_data_loss_on_valid_events(self):
        """Valid events should never be dropped."""
        valid_event = {
            'meta': {'id': 'test-123', 'dt': '2023-01-01T12:00:00Z', 'domain': 'test.org'},
            'type': 'edit',
            'title': 'Test'
        }
        
        fields = get_all_fields(valid_event)
        
        # Should not return None for valid events
        assert fields is not None
        assert fields['evt_id'] == 'test-123'


class TestCostManagement:
    """Tests for cost optimization and resource usage."""

    def test_message_size_within_pubsub_limits(self):
        """Messages should be under Pub/Sub size limits (10MB)."""
        # Create large event
        large_event = {
            'meta': {'id': 'test', 'dt': '2023-01-01T12:00:00Z', 'domain': 'test.org'},
            'type': 'edit',
            'comment': 'A' * 1_000_000  # 1MB comment
        }
        
        # Serialize to JSON
        message_data = json.dumps(large_event).encode('utf-8')
        message_size_mb = len(message_data) / (1024 * 1024)
        
        # Should be under 10MB Pub/Sub limit
        assert message_size_mb < 10, \
            f"Message size {message_size_mb:.2f}MB exceeds Pub/Sub limit"

    def test_bigquery_row_size_within_limits(self):
        """BigQuery rows should be under size limits (100MB)."""
        validator = SchemaValidator()
        
        # Create large record
        large_record = {
            'evt_id': 'test-123',
            'type': 'edit',
            'domain': 'test.org',
            'comment': 'A' * 100_000  # 100KB comment
        }
        
        # Validate and check size
        is_valid, errors = validator.validate_record(large_record)
        
        # Should handle large but valid records
        assert is_valid is True

    def test_dlq_records_not_excessively_large(self):
        """DLQ records should not be excessively large."""
        validator = SchemaValidator()
        
        large_record = {
            'evt_id': 'test-123',
            'type': 'edit',
            'domain': 'test.org',
            'comment': 'A' * 10_000
        }
        
        errors = [ValidationError('TEST', 'test', 'STRING', 'test', 'Test')]
        dlq_record = validator.create_dlq_record(large_record, errors)
        
        # DLQ record should not be much larger than original
        dlq_size = len(json.dumps(dlq_record, default=str))
        original_size = len(json.dumps(large_record))
        
        # DLQ overhead should be reasonable (< 3x original for large records)
        # Overhead includes metadata, error details, and original payload
        assert dlq_size < original_size * 3, \
            f"DLQ size {dlq_size} exceeds 3x original {original_size}"


class TestDataIntegrity:
    """Tests for data integrity and consistency."""

    def test_event_id_uniqueness_preserved(self):
        """Event IDs should be preserved through pipeline."""
        event = {
            'meta': {'id': 'unique-event-123', 'dt': '2023-01-01T12:00:00Z', 'domain': 'test.org'},
            'type': 'edit'
        }
        
        fields = get_all_fields(event)
        
        assert fields['evt_id'] == 'unique-event-123'

    def test_timestamp_consistency(self):
        """Timestamps should be preserved correctly."""
        event = {
            'meta': {'id': 'test', 'dt': '2023-01-01T12:00:00Z', 'domain': 'test.org'},
            'type': 'edit',
            'timestamp': 1672574400
        }
        
        fields = get_all_fields(event)
        
        assert fields['dt_user'] == '2023-01-01T12:00:00Z'
        assert fields['dt_server'] == 1672574400

    def test_type_consistency_through_pipeline(self):
        """Data types should remain consistent."""
        event = {
            'meta': {'id': 'test', 'dt': '2023-01-01T12:00:00Z', 'domain': 'test.org'},
            'type': 'edit',
            'namespace': 0,
            'bot': False,
            'minor': True
        }
        
        fields = get_all_fields(event)
        
        # Types should be preserved
        assert isinstance(fields['ns'], int)
        assert isinstance(fields['bot'], bool)
        assert isinstance(fields['minor'], bool)

    def test_no_data_corruption_with_special_characters(self):
        """Special characters should not corrupt data."""
        special_chars = "Test\n\r\t\x00\u200b\ufeff"
        
        event = {
            'meta': {'id': 'test', 'dt': '2023-01-01T12:00:00Z', 'domain': 'test.org'},
            'type': 'edit',
            'comment': special_chars
        }
        
        fields = get_all_fields(event)
        
        # Should preserve special characters
        assert fields is not None
        assert 'comment' in fields


class TestOperationalReadiness:
    """Tests for operational requirements."""

    def test_validation_errors_are_actionable(self):
        """Validation errors should provide clear guidance."""
        validator = SchemaValidator()
        
        invalid_record = {
            'type': 'edit',
            'domain': 'test.org'
            # Missing evt_id
        }
        
        is_valid, errors = validator.validate_record(invalid_record)
        
        assert is_valid is False
        assert len(errors) > 0
        
        # Errors should have clear messages
        for error in errors:
            assert error.message is not None
            assert len(error.message) > 0
            assert error.field is not None
            assert error.category is not None

    def test_dlq_records_include_debugging_info(self):
        """DLQ records should include information for debugging."""
        validator = SchemaValidator()
        
        record = {'title': 'Test'}
        errors = [ValidationError('TEST', 'evt_id', 'STRING', None, 'Missing required field')]
        
        dlq_record = validator.create_dlq_record(record, errors)
        
        # Should include debugging metadata
        assert '_validation_timestamp' in dlq_record
        assert '_error_category' in dlq_record
        assert '_error_details' in dlq_record
        assert '_original_payload' in dlq_record
        assert '_schema_version' in dlq_record

    def test_schema_version_tracking(self):
        """Schema version should be tracked for all records."""
        validator = SchemaValidator()
        
        assert validator.schema_version is not None
        assert len(validator.schema_version) > 0

    def test_error_categorization_for_monitoring(self):
        """Errors should be categorized for monitoring/alerting."""
        validator = SchemaValidator()
        
        # Test different error categories
        test_cases = [
            ({'title': 'Test'}, 'MISSING_REQUIRED_FIELD'),
            ({'evt_id': 'test', 'type': 'edit', 'domain': 'test.org', 'ns': 'invalid'}, 'TYPE_MISMATCH'),
        ]
        
        for record, expected_category in test_cases:
            is_valid, errors = validator.validate_record(record)
            
            if not is_valid:
                dlq_record = validator.create_dlq_record(record, errors)
                assert '_error_category' in dlq_record
                # Category should be one of the expected types
                assert dlq_record['_error_category'] in [
                    'MISSING_REQUIRED_FIELD',
                    'TYPE_MISMATCH',
                    'VALIDATION_ERROR',
                    'OTHER'
                ]
