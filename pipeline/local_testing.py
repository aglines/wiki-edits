"""Local pipeline testing with sample data."""
import json
from functools import partial

import apache_beam as beam
from apache_beam.io import WriteToText
from apache_beam.io.gcp.pubsub import PubsubMessage
from apache_beam.options.pipeline_options import PipelineOptions

from wiki_pipeline.transforms.filters import AllEvents, MediaFilter
from wiki_pipeline.transforms.extractors import get_all_fields
from wiki_pipeline.utils import clean_boolean_field, get_logger

logger = get_logger(__name__)


def create_sample_events(max_events: int = 20) -> list:
    """Create sample Wikipedia edit events for testing."""
    return [
        {
            'meta': {'id': f'event-{i}', 'dt': '2023-01-01T12:00:00Z', 'domain': 'en.wikipedia.org'},
            'type': 'edit',
            'title': f'Test Article {i}',
            'user': f'TestUser{i}',
            'bot': i % 5 == 0,
            'timestamp': 1672574400 + i,
            'namespace': 6 if i % 10 == 0 else 0,
            'wiki': 'enwiki',
            'server_name': 'en.wikipedia.org',
            'minor': i % 3 == 0,
            'patrolled': i % 4 == 0,
            'length': {'old': 100 + i, 'new': 150 + i}
        }
        for i in range(max_events)
    ]


def run_local_pipeline(max_events: int = 20) -> None:
    """Run pipeline locally with sample data for testing."""
    logger.info("Running local pipeline with sample data...")
    
    sample_events = create_sample_events(max_events)
    pubsub_messages = [
        PubsubMessage(data=json.dumps(event).encode('utf-8'), attributes={})
        for event in sample_events
    ]
    
    options = PipelineOptions(['--runner=DirectRunner'])
    
    with beam.Pipeline(options=options) as p:
        (p | 'Create Sample Data' >> beam.Create(pubsub_messages)
           | 'Filter Events' >> beam.ParDo(AllEvents())
           | 'Extract Fields' >> beam.Map(get_all_fields)
           | 'Filter Media' >> beam.ParDo(MediaFilter())
           | 'Clean Minor Flag' >> beam.Map(partial(clean_boolean_field, key='minor'))
           | 'Clean Patrol Flag' >> beam.Map(partial(clean_boolean_field, key='patrolled'))
           | 'Format as JSON' >> beam.Map(lambda x: json.dumps(x, default=str))
           | 'Write to File' >> WriteToText('output/processed_events', file_name_suffix='.jsonl'))
    
    logger.info("✅ Local pipeline completed - check output/ directory")
