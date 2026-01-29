"""Local testing utilities for the Wikipedia edits pipeline."""
import json
import logging
from typing import List, Dict, Any
from datetime import datetime

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.io.gcp.pubsub import PubsubMessage

from src.wiki_pipeline.transforms.filters import AllEvents, MediaFilter
from src.wiki_pipeline.transforms.extractors import get_all_fields
from wiki_pipeline.utils import clean_boolean_field, setup_logging, get_logger

logger = get_logger(__name__)


def create_test_message(event_type: str = 'edit', namespace: int = 0) -> bytes:
    """Create a test Wikipedia edit message."""
    test_data = {
        'meta': {
            'id': 'test-event-123',
            'dt': '2023-01-01T12:00:00Z',
            'domain': 'en.wikipedia.org'
        },
        'type': event_type,
        'title': 'Test Article',
        'user': 'TestUser',
        'bot': False,
        'timestamp': 1672574400,
        'namespace': namespace,
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
    return json.dumps(test_data).encode('utf-8')


def create_pubsub_message(data: bytes) -> PubsubMessage:
    """Create a mock Pub/Sub message for testing."""
    return PubsubMessage(data=data, attributes={})


def test_filters():
    """Test the filtering components."""
    logger.info("Testing filters...")
    
    # Test AllEvents filter
    test_msg = create_pubsub_message(create_test_message())
    canary_data = {
        'meta': {'id': 'canary', 'dt': '2023-01-01T12:00:00Z', 'domain': 'canary'},
        'type': 'edit'
    }
    canary_msg = create_pubsub_message(json.dumps(canary_data).encode('utf-8'))
    
    with TestPipeline() as p:
        messages = p | beam.Create([test_msg, canary_msg])
        filtered = messages | beam.ParDo(AllEvents())
        
        def check_filter_results(results):
            assert len(results) == 1  # Only non-canary message should pass
            assert results[0]['meta']['domain'] == 'en.wikipedia.org'
        
        assert_that(filtered, check_filter_results)
    
    logger.info("✓ Filters test passed")


def test_extractors():
    """Test field extraction."""
    logger.info("Testing extractors...")
    
    test_data = json.loads(create_test_message().decode('utf-8'))
    result = get_all_fields(test_data)
    
    assert result is not None
    assert result['evt_id'] == 'test-event-123'
    assert result['type'] == 'edit'
    assert result['title'] == 'Test Article'
    assert result['minor'] == True
    
    logger.info("✓ Extractors test passed")


def test_converters():
    """Test data converters."""
    logger.info("Testing converters...")
    
    # Test boolean cleaner
    test_data = {'minor': True, 'other': 'value'}
    result = clean_boolean_field(test_data, 'minor')
    assert result['minor'] == True
    
    # Test boolean field cleaning
    test_data = {'minor': 'true', 'patrolled': 1}
    result = clean_boolean_field(test_data, 'minor')
    assert result['minor'] == True
    
    logger.info("✓ Converters test passed")


def run_local_pipeline_test():
    """Run a complete pipeline test locally."""
    logger.info("Running local pipeline test...")
    
    # Create test data
    test_messages = [
        create_pubsub_message(create_test_message('edit', 0)),
        create_pubsub_message(create_test_message('edit', 6)),  # Media file
        create_pubsub_message(create_test_message('new', 0)),
    ]
    
    options = PipelineOptions([
        '--runner=DirectRunner',
        '--project=test-project'
    ])
    
    with TestPipeline(options=options) as p:
        # Simulate the main pipeline steps
        results = (p 
                  | 'Create Test Data' >> beam.Create(test_messages)
                  | 'Filter Events' >> beam.ParDo(AllEvents())
                  | 'Extract Fields' >> beam.Map(get_all_fields)
                  | 'Filter None' >> beam.Filter(lambda x: x is not None)
                  | 'Filter Media' >> beam.ParDo(MediaFilter())
                  | 'Clean Minor Flag' >> beam.Map(lambda x: clean_boolean_field(x, 'minor'))
                  | 'Clean Patrol Flag' >> beam.Map(lambda x: clean_boolean_field(x, 'patrolled')))
        
        def validate_results(results):
            assert len(results) >= 2  # Should have processed multiple events
            for result in results:
                assert 'evt_id' in result
                assert 'type' in result
                assert 'dt_server' in result
        
        assert_that(results, validate_results)
    
    logger.info("✓ Local pipeline test passed")


def main():
    """Run all tests."""
    setup_logging(level="INFO", structured=False)
    
    logger.info("Starting pipeline tests...")
    
    try:
        test_filters()
        test_extractors() 
        test_converters()
        run_local_pipeline_test()
        
        logger.info("🎉 All tests passed!")
        
    except Exception as e:
        logger.error(f"❌ Tests failed: {e}")
        raise


if __name__ == '__main__':
    main()
