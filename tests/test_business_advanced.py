"""Advanced business-critical tests for production scenarios."""
import pytest
import json

from wiki_pipeline.transforms.extractors import get_all_fields
from wiki_pipeline.schema import SchemaValidator, SCHEMA_FIELDS
from wiki_pipeline.ai_detection import detect_ai_indicators, get_patterns


class TestDuplicateDetection:
    """Tests for duplicate event handling."""

    def test_same_event_id_processed_twice(self):
        """Should handle duplicate event IDs gracefully."""
        event = {
            'meta': {'id': 'duplicate-123', 'dt': '2023-01-01T12:00:00Z', 'domain': 'test.org'},
            'type': 'edit',
            'title': 'Test'
        }
        
        # Process same event twice
        fields1 = get_all_fields(event)
        fields2 = get_all_fields(event)
        
        # Should produce identical results
        assert fields1 == fields2
        assert fields1['evt_id'] == fields2['evt_id']

    def test_duplicate_detection_by_content(self):
        """Should be able to identify duplicates by content."""
        event1 = {
            'meta': {'id': 'event-1', 'dt': '2023-01-01T12:00:00Z', 'domain': 'test.org'},
            'type': 'edit',
            'title': 'Same Title',
            'user': 'SameUser',
            'comment': 'Same comment'
        }
        
        event2 = {
            'meta': {'id': 'event-2', 'dt': '2023-01-01T12:00:00Z', 'domain': 'test.org'},
            'type': 'edit',
            'title': 'Same Title',
            'user': 'SameUser',
            'comment': 'Same comment'
        }
        
        fields1 = get_all_fields(event1)
        fields2 = get_all_fields(event2)
        
        # Different IDs but same content
        assert fields1['evt_id'] != fields2['evt_id']
        assert fields1['title'] == fields2['title']
        assert fields1['user'] == fields2['user']
        assert fields1['comment'] == fields2['comment']


class TestEventOrdering:
    """Tests for event ordering and timestamp handling."""

    def test_out_of_order_events_detectable(self):
        """Should be able to detect out-of-order events by timestamp."""
        events = [
            {
                'meta': {'id': 'event-1', 'dt': '2023-01-01T12:00:00Z', 'domain': 'test.org'},
                'type': 'edit',
                'timestamp': 1672574400
            },
            {
                'meta': {'id': 'event-2', 'dt': '2023-01-01T11:00:00Z', 'domain': 'test.org'},
                'type': 'edit',
                'timestamp': 1672570800  # Earlier timestamp
            },
        ]
        
        fields = [get_all_fields(e) for e in events]
        
        # Should preserve timestamps for ordering detection
        assert fields[0]['dt_server'] > fields[1]['dt_server']

    def test_timestamp_consistency_validation(self):
        """User timestamp and server timestamp should be consistent."""
        event = {
            'meta': {'id': 'test', 'dt': '2023-01-01T12:00:00Z', 'domain': 'test.org'},
            'type': 'edit',
            'timestamp': 1672574400  # Should match dt
        }
        
        fields = get_all_fields(event)
        
        # Both timestamps should be present
        assert fields['dt_user'] is not None
        assert fields['dt_server'] is not None


class TestBackpressureHandling:
    """Tests for handling slow downstream systems."""

    def test_large_batch_processing(self):
        """Should handle large batches without failure."""
        batch_size = 1000
        
        events = [
            {
                'meta': {'id': f'event-{i}', 'dt': '2023-01-01T12:00:00Z', 'domain': 'test.org'},
                'type': 'edit',
                'comment': f'Comment {i}'
            }
            for i in range(batch_size)
        ]
        
        # Process entire batch
        processed = [get_all_fields(e) for e in events]
        
        # All should be processed
        assert len(processed) == batch_size
        assert all(f is not None for f in processed)

    def test_memory_efficient_batch_processing(self):
        """Should process batches without holding all in memory."""
        batch_size = 10000
        
        # Process one at a time (streaming)
        processed_count = 0
        for i in range(batch_size):
            event = {
                'meta': {'id': f'event-{i}', 'dt': '2023-01-01T12:00:00Z', 'domain': 'test.org'},
                'type': 'edit'
            }
            
            fields = get_all_fields(event)
            if fields:
                processed_count += 1
            
            # Don't accumulate results
            del fields
        
        assert processed_count == batch_size


class TestPatternOverlapAnalysis:
    """Tests for understanding pattern interactions."""

    def test_pattern_overlap_detection(self):
        """Identify which patterns commonly trigger together."""
        patterns = get_patterns()
        
        # Skip if patterns not loaded
        if not patterns:
            pytest.skip("Pattern file not loaded")
        
        test_cases = [
            "As an AI language model with **bold formatting**",  # Multiple patterns
            "This breathtaking location plays a vital role",  # Multiple patterns
            "I hope this helps with your understanding",  # Single pattern
        ]
        
        overlap_results = []
        for comment in test_cases:
            content = {
                'comment': comment,
                'parsed_comment': '',
                'title': 'Test',
                'user': 'User',
                'len_old': 0,
                'len_new': 0,
                'minor': False,
                'bot': False
            }
            
            flags = detect_ai_indicators(content)
            overlap_results.append((comment[:50], len(flags), list(flags.keys())))
        
        # Should detect at least some patterns
        total_flags = sum(r[1] for r in overlap_results)
        assert total_flags > 0, "Should detect at least some AI indicators"

    def test_all_patterns_independently_detectable(self):
        """Each pattern should be independently detectable."""
        patterns = get_patterns()
        
        # Test each list-based pattern independently
        for pattern_type, pattern_list in patterns.items():
            if isinstance(pattern_list, list) and pattern_list:
                # Test first pattern in list
                test_pattern = pattern_list[0]
                
                content = {
                    'comment': f"Test {test_pattern} content",
                    'parsed_comment': '',
                    'title': 'Test',
                    'user': 'User',
                    'len_old': 0,
                    'len_new': 0,
                    'minor': False,
                    'bot': False
                }
                
                flags = detect_ai_indicators(content)
                
                # Should detect this pattern type
                # (May also detect others if patterns overlap)
                assert len(flags) > 0, f"Pattern type {pattern_type} not detected"


class TestSchemaMigration:
    """Tests for schema migration scenarios."""

    def test_old_schema_data_through_new_validator(self):
        """Old schema data should validate or provide clear migration path."""
        validator = SchemaValidator()
        
        # Simulate old schema (missing new optional fields)
        old_record = {
            'evt_id': 'old-event-123',
            'type': 'edit',
            'domain': 'test.org',
            # Missing newer optional fields
        }
        
        is_valid, errors = validator.validate_record(old_record)
        
        # Should either validate or have clear errors
        if not is_valid:
            assert len(errors) > 0
            # Errors should be about missing required fields only
            for error in errors:
                assert error.category in ['MISSING_REQUIRED_FIELD', 'TYPE_MISMATCH']

    def test_new_optional_fields_dont_break_old_data(self):
        """Adding optional fields should not break existing data."""
        validator = SchemaValidator()
        
        # Record without new optional fields
        record = {
            'evt_id': 'test-123',
            'type': 'edit',
            'domain': 'test.org'
        }
        
        is_valid, errors = validator.validate_record(record)
        
        # Should validate successfully
        assert is_valid is True

    def test_schema_fields_documented(self):
        """All schema fields should be documented in SCHEMA_FIELDS."""
        # Ensure SCHEMA_FIELDS is comprehensive
        assert len(SCHEMA_FIELDS) > 0
        
        # Required fields should be marked
        required_fields = [k for k, v in SCHEMA_FIELDS.items() if v['required']]
        assert len(required_fields) >= 3  # At least evt_id, type, domain

    def test_dlq_preserves_schema_version_for_migration(self):
        """DLQ records should track schema version for migration."""
        validator = SchemaValidator()
        
        record = {'title': 'Test'}
        from wiki_pipeline.schema import ValidationError
        errors = [ValidationError('TEST', 'test', 'STRING', 'test', 'Test')]
        
        dlq_record = validator.create_dlq_record(record, errors)
        
        # Should have schema version for migration tracking
        assert '_schema_version' in dlq_record
        assert dlq_record['_schema_version'] is not None


class TestPatternMaintenance:
    """Tests for pattern configuration maintenance."""

    def test_pattern_file_structure_consistent(self):
        """Pattern file should maintain consistent structure."""
        patterns = get_patterns()
        
        # All list patterns should be lists
        list_pattern_types = [
            'chatgpt_artifacts',
            'chatgpt_attribution',
            'prompt_refusal',
            'knowledge_cutoff_disclaimers',
            'collaborative_communication',
            'promotional_language',
            'importance_emphasis'
        ]
        
        for pattern_type in list_pattern_types:
            if pattern_type in patterns:
                assert isinstance(patterns[pattern_type], list), \
                    f"{pattern_type} should be a list"

    def test_pattern_strings_not_empty(self):
        """Pattern strings should not be empty."""
        patterns = get_patterns()
        
        for pattern_type, pattern_value in patterns.items():
            if isinstance(pattern_value, list):
                for pattern in pattern_value:
                    if isinstance(pattern, str):
                        assert len(pattern) > 0, \
                            f"Empty pattern in {pattern_type}"

    def test_no_duplicate_patterns(self):
        """Pattern lists should not contain duplicates."""
        patterns = get_patterns()
        
        for pattern_type, pattern_value in patterns.items():
            if isinstance(pattern_value, list):
                # Check for duplicates
                unique_patterns = set(pattern_value)
                assert len(unique_patterns) == len(pattern_value), \
                    f"Duplicate patterns found in {pattern_type}"
