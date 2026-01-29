"""Unit tests for pipeline transforms."""
import json
import pytest
from apache_beam.io.gcp.pubsub import PubsubMessage

from wiki_pipeline.transforms.filters import AllEvents, MediaFilter
from wiki_pipeline.transforms.extractors import (
    get_all_fields,
    extract_core_fields,
    extract_edit_fields,
)
from wiki_pipeline.utils import clean_boolean_field

# Common fixtures now in conftest.py: sample_edit_event, sample_canary_event, sample_media_event


class TestAllEvents:
    """Tests for AllEvents filter."""

    def test_filters_valid_event(self, sample_edit_event):
        """Valid events should pass through."""
        msg = PubsubMessage(
            data=json.dumps(sample_edit_event).encode('utf-8'),
            attributes={}
        )
        filter_fn = AllEvents()
        results = list(filter_fn.process(msg))
        
        assert len(results) == 1
        assert results[0]['meta']['id'] == 'test-event-123'

    def test_filters_canary_event(self, sample_canary_event):
        """Canary events should be filtered out."""
        msg = PubsubMessage(
            data=json.dumps(sample_canary_event).encode('utf-8'),
            attributes={}
        )
        filter_fn = AllEvents()
        results = list(filter_fn.process(msg))
        
        assert len(results) == 0

    def test_handles_invalid_json(self):
        """Invalid JSON should be handled gracefully."""
        msg = PubsubMessage(data=b'invalid json', attributes={})
        filter_fn = AllEvents()
        results = list(filter_fn.process(msg))
        
        assert len(results) == 0

    def test_handles_missing_meta(self):
        """Events without meta field should be filtered."""
        invalid_event = {'type': 'edit', 'title': 'Test'}
        msg = PubsubMessage(
            data=json.dumps(invalid_event).encode('utf-8'),
            attributes={}
        )
        filter_fn = AllEvents()
        results = list(filter_fn.process(msg))
        
        assert len(results) == 0


class TestMediaFilter:
    """Tests for MediaFilter."""

    def test_passes_non_media_events(self, sample_edit_event):
        """Non-media events should pass unchanged."""
        filter_fn = MediaFilter()
        results = list(filter_fn.process(sample_edit_event))
        
        assert len(results) == 1
        assert results[0] == sample_edit_event

    def test_filters_media_log_params(self, sample_media_event):
        """Media events should have log_params removed."""
        filter_fn = MediaFilter()
        results = list(filter_fn.process(sample_media_event))
        
        assert len(results) == 1
        assert 'log_params' not in results[0]
        assert results[0]['namespace'] == 6

    def test_preserves_media_metadata(self, sample_media_event):
        """Media events should keep other metadata."""
        filter_fn = MediaFilter()
        results = list(filter_fn.process(sample_media_event))
        
        assert len(results) == 1
        assert results[0]['title'] == 'File:Example.jpg'
        assert results[0]['user'] == 'MediaUser'


class TestFieldExtractors:
    """Tests for field extraction functions."""

    def test_extract_core_fields(self, sample_edit_event):
        """Core fields should be extracted correctly."""
        result = extract_core_fields(sample_edit_event)
        
        assert result['evt_id'] == 'test-event-123'
        assert result['type'] == 'edit'
        assert result['title'] == 'Test Article'
        assert result['user'] == 'TestUser'
        assert result['bot'] is False
        assert result['domain'] == 'en.wikipedia.org'
        assert result['ns'] == 0

    def test_extract_edit_fields(self, sample_edit_event):
        """Edit-specific fields should be extracted."""
        result = extract_edit_fields(sample_edit_event)
        
        assert result['minor'] is True
        assert result['patrolled'] is False
        assert result['len_old'] == 100
        assert result['len_new'] == 150
        assert result['rev_old'] == 12345
        assert result['rev_new'] == 12346
        assert result['comment'] == 'Test edit'

    def test_get_all_fields(self, sample_edit_event):
        """get_all_fields should combine all extractors."""
        result = get_all_fields(sample_edit_event)
        
        assert result is not None
        assert 'evt_id' in result
        assert 'minor' in result
        assert result['type'] == 'edit'

    def test_get_all_fields_handles_missing_data(self):
        """get_all_fields should handle incomplete events."""
        incomplete_event = {
            'meta': {'id': 'test', 'dt': '2023-01-01T12:00:00Z', 'domain': 'test.org'},
            'type': 'edit'
        }
        result = get_all_fields(incomplete_event)
        
        assert result is not None
        assert result['evt_id'] == 'test'


class TestCleanBooleanField:
    """Tests for boolean field cleaning."""

    def test_cleans_true_boolean(self):
        """True boolean should remain True."""
        data = {'minor': True, 'other': 'value'}
        result = clean_boolean_field(data, 'minor')
        
        assert result['minor'] is True
        assert isinstance(result['minor'], bool)

    def test_cleans_false_boolean(self):
        """False boolean should remain False."""
        data = {'minor': False}
        result = clean_boolean_field(data, 'minor')
        
        assert result['minor'] is False
        assert isinstance(result['minor'], bool)

    def test_cleans_truthy_value(self):
        """Truthy values should become True."""
        data = {'minor': 1}
        result = clean_boolean_field(data, 'minor')
        
        assert result['minor'] is True

    def test_cleans_falsy_value(self):
        """Falsy values should become False."""
        data = {'minor': 0}
        result = clean_boolean_field(data, 'minor')
        
        assert result['minor'] is False

    def test_handles_missing_field(self):
        """Missing fields should not cause errors."""
        data = {'other': 'value'}
        result = clean_boolean_field(data, 'minor')
        
        assert 'minor' not in result
