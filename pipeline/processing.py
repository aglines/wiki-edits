"""Dataflow pipeline for processing Pub/Sub events to BigQuery."""
import logging
from datetime import datetime
from functools import partial

import apache_beam as beam
from apache_beam.io import ReadFromPubSub, WriteToBigQuery
from apache_beam.io.gcp.bigquery import BigQueryDisposition
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions

from wiki_pipeline.schema import BQ_SCHEMA, DLQ_SCHEMA, AI_SCHEMA
from wiki_pipeline.schema_migration import ensure_pipeline_tables
from wiki_pipeline.transforms.filters import AllEvents, MediaFilter, AIContentMonitor
from wiki_pipeline.transforms.extractors import get_all_fields
from wiki_pipeline.utils import clean_boolean_field
from wiki_pipeline.transforms.validation import ValidationTransform

logger = logging.getLogger(__name__)


def run_dataflow_pipeline(config: dict) -> None:
    """Run the streaming Dataflow pipeline."""
    logger.info("Starting streaming Dataflow pipeline...")
    logger.info("📋 Assuming BigQuery tables already exist (run setup_tables.py first)")
    
    # Log scaling configuration for monitoring
    logger.info(f"🔧 Scaling: {config['num_workers']}-{config['max_workers']} workers")
    logger.info(f"💻 Machine: {config['machine_type']} with {config['disk_size_gb']}GB SSD")
    logger.info(f"⚡ Streaming Engine: {config.get('enable_streaming_engine', 'true')}")
    
    # Build pipeline options with configurable scaling
    pipeline_args = [
        f'--project={config["project_id"]}',
        f'--region={config["region"]}',
        f'--job_name={config["job_name"]}-{datetime.now().strftime("%y%m%d-%H%M%S")}',
        f'--temp_location={config["temp_location"]}',
        '--runner=DataflowRunner',
        '--streaming',
        # Configurable scaling parameters
        f'--worker_machine_type={config["machine_type"]}',
        f'--max_num_workers={config["max_workers"]}',
        f'--num_workers={config["num_workers"]}',
        f'--disk_size_gb={config["disk_size_gb"]}',
        '--autoscaling_algorithm=THROUGHPUT_BASED',  # Scale based on throughput
        '--worker_disk_type=compute.googleapis.com/projects//zones//diskTypes/pd-ssd',
        # Advanced scaling parameters
        # '--target_parallelism=4',                # Target parallelism per worker
        # '--max_bundle_size=1000',                # Optimize bundle sizes
        # '--max_bundle_time_seconds=10',          # Process bundles more frequently
        '--setup_file=./setup.py'
    ]
    
    # Add streaming engine if enabled
    if config.get('enable_streaming_engine', 'true').lower() == 'true':
        pipeline_args.append('--enable_streaming_engine')
        logger.info("🚀 Streaming Engine enabled for better performance")
    
    opts = PipelineOptions(pipeline_args)
    
    # Configure setup options for proper module distribution
    opts.view_as(SetupOptions).save_main_session = True
    
    with beam.Pipeline(options=opts) as p:
        # Main processing pipeline
        events = (p | 'Read from Pub/Sub' >> ReadFromPubSub(subscription=config['subscription'], with_attributes=True))
        
        # AI Content Monitoring with side outputs
        ai_monitor_results = (events
                             | 'Filter Events' >> beam.ParDo(AllEvents())
                             | 'Extract Fields' >> beam.Map(get_all_fields)
                             | 'Filter Media' >> beam.ParDo(MediaFilter())
                             | 'AI Content Monitor' >> beam.ParDo(AIContentMonitor()).with_outputs('ai_events', main='main'))

        # Main pipeline continues with events (including ai_flags)
        filtered_events = (ai_monitor_results.main
                          | 'Clean Minor Flag' >> beam.Map(partial(clean_boolean_field, key='minor'))
                          | 'Clean Patrol Flag' >> beam.Map(partial(clean_boolean_field, key='patrolled')))

        # AI events go to separate tracking table
        ai_events = ai_monitor_results.ai_events
        
        # Validation with multiple outputs
        validation_results = filtered_events | 'Validate Data' >> beam.ParDo(ValidationTransform()).with_outputs('valid', 'invalid')
        
        # Write valid data to main table
        valid_data = validation_results.valid | 'Log Valid Records' >> beam.Map(lambda x: logging.info(f"Sending to BQ: {x.get('evt_id', 'unknown')}") or x)
        
        write_result = (valid_data 
                       | 'Remove AI Flags' >> beam.Map(lambda x: {k: v for k, v in x.items() if k != 'ai_flags'})
                       | 'Write Valid to BigQuery' >> WriteToBigQuery(
                           config['output_table'],
                           schema=BQ_SCHEMA,
                           create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                           write_disposition=BigQueryDisposition.WRITE_APPEND))
        
        # Write invalid data to dead letter table with structured schema
        (validation_results.invalid
         | 'Write Invalid to Dead Letter' >> WriteToBigQuery(
             f"{config['output_table']}_dead_letter",
             schema=DLQ_SCHEMA,
             create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
             write_disposition=BigQueryDisposition.WRITE_APPEND))

        # Write AI events to tracking table
        (ai_events
         | 'Write AI Events' >> WriteToBigQuery(
             f"{config['output_table']}_ai_events",
             schema=AI_SCHEMA,
             create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
             write_disposition=BigQueryDisposition.WRITE_APPEND))
    
    logger.info("Pipeline completed successfully")
