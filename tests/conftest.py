"""Pytest configuration and shared fixtures."""
import sys
import os
import pytest

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))


# ============================================================================
# Common test fixtures
# ============================================================================

@pytest.fixture
def sample_edit_event():
    """Sample Wikipedia edit event."""
    return {
        'meta': {
            'id': 'test-event-123',
            'dt': '2023-01-01T12:00:00Z',
            'domain': 'en.wikipedia.org'
        },
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


@pytest.fixture
def sample_canary_event():
    """Sample canary event that should be filtered."""
    return {
        'meta': {
            'id': 'canary-123',
            'dt': '2023-01-01T12:00:00Z',
            'domain': 'canary'
        },
        'type': 'edit'
    }


@pytest.fixture
def sample_media_event():
    """Sample media file event (namespace 6)."""
    return {
        'meta': {
            'id': 'media-event-456',
            'dt': '2023-01-01T12:00:00Z',
            'domain': 'en.wikipedia.org'
        },
        'type': 'edit',
        'title': 'File:Example.jpg',
        'namespace': 6,
        'user': 'MediaUser',
        'bot': False,
        'timestamp': 1672574400,
        'wiki': 'enwiki',
        'server_name': 'en.wikipedia.org',
        'log_params': {'large_data': 'should_be_removed'}
    }
