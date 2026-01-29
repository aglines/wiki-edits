#!/usr/bin/env python3
"""Debug script to check pipeline data flow and Pub/Sub status."""

import json
import time
from google.cloud import pubsub_v1
from google.cloud import bigquery
from wiki_pipeline.utils import validate_config, setup_logging, get_logger

logger = get_logger(__name__)

def check_pubsub_topic(project_id: str, topic_name: str):
    """Check Pub/Sub topic status and message count."""
    subscriber = pubsub_v1.SubscriberClient()
    publisher = pubsub_v1.PublisherClient()
    
    # Extract topic name from full path
    if topic_name.startswith('projects/'):
        topic_parts = topic_name.split('/')
        topic_short_name = topic_parts[-1]
    else:
        topic_short_name = topic_name
    
    # Create a temporary subscription to check messages
    subscription_name = f"{topic_short_name}-debug-{int(time.time())}"
    subscription_path = subscriber.subscription_path(project_id, subscription_name)
    
    try:
        # Create temporary subscription
        subscription = subscriber.create_subscription(
            request={"name": subscription_path, "topic": topic_name}
        )
        logger.info(f"Created debug subscription: {subscription_name}")
        
        # Pull messages (non-blocking)
        response = subscriber.pull(
            request={"subscription": subscription_path, "max_messages": 10},
            timeout=5.0
        )
        
        message_count = len(response.received_messages)
        logger.info(f"Found {message_count} messages in topic")
        
        # Log sample messages
        for i, received_message in enumerate(response.received_messages[:3]):
            try:
                data = json.loads(received_message.message.data.decode('utf-8'))
                logger.info(f"Sample message {i+1}: type={data.get('type')}, domain={data.get('meta', {}).get('domain')}")
            except Exception as e:
                logger.error(f"Failed to parse message {i+1}: {e}")
        
        # Acknowledge messages
        if response.received_messages:
            ack_ids = [msg.ack_id for msg in response.received_messages]
            subscriber.acknowledge(request={"subscription": subscription_path, "ack_ids": ack_ids})
            
    except Exception as e:
        logger.error(f"Error checking Pub/Sub topic: {e}")
    finally:
        # Clean up subscription
        try:
            subscriber.delete_subscription(request={"subscription": subscription_path})
            logger.info(f"Deleted debug subscription: {subscription_name}")
        except Exception as e:
            logger.error(f"Failed to delete debug subscription: {e}")

def check_bigquery_table(table_id: str):
    """Check BigQuery table status."""
    client = bigquery.Client()
    
    try:
        # Parse table ID format: project:dataset.table
        if ':' in table_id and '.' in table_id:
            project_dataset, table_name = table_id.split('.')
            project_id, dataset_id = project_dataset.split(':')
            full_table_id = f"{project_id}.{dataset_id}.{table_name}"
        else:
            full_table_id = table_id
            
        table = client.get_table(full_table_id)
        logger.info(f"BigQuery table exists: {full_table_id}")
        logger.info(f"Table schema has {len(table.schema)} fields")
        logger.info(f"Table has {table.num_rows} rows")
        
        # Try a simple query
        query = f"SELECT COUNT(*) as row_count FROM `{full_table_id}` LIMIT 1"
        results = client.query(query)
        for row in results:
            logger.info(f"Current row count: {row.row_count}")
            
    except Exception as e:
        logger.error(f"Error checking BigQuery table: {e}")

def main():
    """Run debugging checks."""
    setup_logging(level="INFO", structured=True)
    
    try:
        config = validate_config()
        logger.info("=== Debugging Pipeline Data Flow ===")
        
        logger.info("1. Checking Pub/Sub topic...")
        check_pubsub_topic(config['project_id'], config['topic'])
        
        logger.info("2. Checking BigQuery table...")
        check_bigquery_table(config['output_table'])
        
        logger.info("=== Debug Complete ===")
        
    except Exception as e:
        logger.error(f"Debug failed: {e}")

if __name__ == '__main__':
    main()
