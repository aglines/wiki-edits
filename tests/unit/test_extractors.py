"""Unit tests for field extraction utilities."""
import pytest
from wiki_pipeline.transforms.extractors import (
    extract_core_fields,
    extract_edit_fields,
    extract_ai_content_fields,
    extract_log_fields,
    get_all_fields,
)
from wiki_pipeline.utils import clean_boolean_field


class TestExtractCoreFields:
    """Test core field extraction."""

    def test_extract_core_fields_success(self):
        """Test successful extraction of core fields."""
        msg = {
            'meta': {'id': 'test-123', 'dt': '2024-01-01T00:00:00Z', 'domain': 'en.wikipedia.org'},
            'type': 'edit',
            'title': 'Test Article',
            'user': 'TestUser',
            'bot': False,
            'timestamp': 1234567890,
            'namespace': 0,
            'wiki': 'enwiki',
            'server_name': 'en.wikipedia.org',
            'server_url': 'https://en.wikipedia.org',
        }

        result = extract_core_fields(msg)

        assert result['evt_id'] == 'test-123'
        assert result['type'] == 'edit'
        assert result['title'] == 'Test Article'
        assert result['user'] == 'TestUser'
        assert result['bot'] is False
        assert result['dt_user'] == '2024-01-01T00:00:00Z'
        assert result['dt_server'] == 1234567890
        assert result['domain'] == 'en.wikipedia.org'
        assert result['ns'] == 0
        assert result['wiki'] == 'enwiki'

    def test_extract_core_fields_missing_optional(self):
        """Test extraction with missing optional fields."""
        msg = {
            'meta': {'id': 'test-456', 'dt': '2024-01-01T00:00:00Z', 'domain': 'en.wikipedia.org'},
            'type': 'edit',
        }

        result = extract_core_fields(msg)

        assert result['evt_id'] == 'test-456'
        assert result['title'] == ''
        assert result['user'] == ''
        assert result['bot'] is False
        assert result['ns'] == 0

    def test_extract_core_fields_missing_required(self):
        """Test extraction fails with missing required fields."""
        msg = {'type': 'edit'}

        with pytest.raises(KeyError):
            extract_core_fields(msg)


class TestExtractEditFields:
    """Test edit field extraction."""

    def test_extract_edit_fields_complete(self):
        """Test extraction with all edit fields present."""
        msg = {
            'minor': True,
            'patrolled': False,
            'length': {'old': 100, 'new': 150},
            'revision': {'old': 12345, 'new': 12346},
            'comment': 'Fixed typo',
            'parsedcomment': '<b>Fixed typo</b>',
        }

        result = extract_edit_fields(msg)

        assert result['minor'] is True
        assert result['patrolled'] is False
        assert result['len_old'] == 100
        assert result['len_new'] == 150
        assert result['rev_old'] == 12345
        assert result['rev_new'] == 12346
        assert result['comment'] == 'Fixed typo'
        assert result['parsed_comment'] == '<b>Fixed typo</b>'

    def test_extract_edit_fields_defaults(self):
        """Test extraction with default values for missing fields."""
        msg = {}

        result = extract_edit_fields(msg)

        assert result['minor'] is False
        assert result['patrolled'] is False
        assert result['len_old'] == 0
        assert result['len_new'] == 0
        assert result['rev_old'] == 0
        assert result['rev_new'] == 0
        assert result['comment'] == ''
        assert result['parsed_comment'] == ''


class TestCleanBooleanField:
    """Test boolean field cleaning."""

    def test_clean_boolean_true(self):
        """Test cleaning truthy values to bool."""
        element = {'minor': 1, 'other': 'data'}
        result = clean_boolean_field(element, 'minor')
        assert result['minor'] is True
        assert isinstance(result['minor'], bool)

    def test_clean_boolean_false(self):
        """Test cleaning falsy values to bool."""
        element = {'minor': 0}
        result = clean_boolean_field(element, 'minor')
        assert result['minor'] is False
        assert isinstance(result['minor'], bool)

    def test_clean_boolean_missing_key(self):
        """Test with missing key returns unchanged."""
        element = {'other': 'data'}
        result = clean_boolean_field(element, 'minor')
        assert 'minor' not in result
        assert result['other'] == 'data'
