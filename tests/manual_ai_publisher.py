#!/usr/bin/env python3
"""
Test script to generate synthetic Wikipedia events with AI content
for testing AI detection pipeline functionality.
"""

import json
import os
import sys
from datetime import datetime
from google.cloud import pubsub_v1

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

def create_test_events():
    """Create synthetic Wikipedia events with AI content patterns."""
    
    base_timestamp = "2024-09-30T10:00:00Z"
    
    # Test events with different AI detection levels
    test_events = [
        {
            "evt_id": "TEST_DATA_DELETE_001",
            "type": "edit", 
            "title": "TEST_DATA_DELETE",
            "user": "TEST_DATA_DELETE",
            "comment": "As an AI language model, I cannot provide personal opinions on this controversial topic.",
            "domain": "en.wikipedia.org",
            "timestamp": base_timestamp,
            "namespace": 0,
            "wiki": "enwiki",
            "server_name": "en.wikipedia.org",
            "server_url": "https://en.wikipedia.org",
            "minor": False,
            "patrolled": False,
            "length": {"old": 1000, "new": 1200},
            "revision": {"old": 12345, "new": 12346},
            "bot": False
        },
        {
            "evt_id": "TEST_DATA_DELETE_002", 
            "type": "edit",
            "title": "TEST_DATA_DELETE",
            "user": "TEST_DATA_DELETE",
            "comment": "Updated article with **bold formatting** and improved structure for better readability.",
            "domain": "en.wikipedia.org", 
            "timestamp": base_timestamp,
            "namespace": 0,
            "wiki": "enwiki",
            "server_name": "en.wikipedia.org",
            "server_url": "https://en.wikipedia.org",
            "minor": False,
            "patrolled": False,
            "length": {"old": 800, "new": 950},
            "revision": {"old": 12347, "new": 12348},
            "bot": False
        },
        {
            "evt_id": "TEST_DATA_DELETE_003",
            "type": "edit",
            "title": "TEST_DATA_DELETE", 
            "user": "TEST_DATA_DELETE",
            "comment": "This location boasts breathtaking views and rich cultural heritage that plays a vital role in the region.",
            "domain": "en.wikipedia.org",
            "timestamp": base_timestamp,
            "namespace": 0,
            "wiki": "enwiki", 
            "server_name": "en.wikipedia.org",
            "server_url": "https://en.wikipedia.org",
            "minor": False,
            "patrolled": False,
            "length": {"old": 1500, "new": 1650},
            "revision": {"old": 12349, "new": 12350},
            "bot": False
        }
    ]
    
    return test_events

def publish_test_events():
    """Publish test events to Pub/Sub topic."""
    
    # Get configuration from environment
    project_id = os.getenv('GCP_PROJECT_ID')
    topic_name = os.getenv('PUBSUB_TOPIC')
    
    print(f"Publishing test events to {project_id}/{topic_name}")
    
    # Initialize Pub/Sub publisher
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_name)
    
    # Create and publish test events
    test_events = create_test_events()
    
    for i, event in enumerate(test_events, 1):
        # Convert to JSON bytes
        message_data = json.dumps(event).encode('utf-8')
        
        # Publish message
        future = publisher.publish(topic_path, message_data)
        message_id = future.result()
        
        print(f"Published test event {i}/3: {event['evt_id']} (Message ID: {message_id})")
        print(f"  AI Content: {event['comment'][:50]}...")
    
    print(f"\nSuccessfully published {len(test_events)} test events!")
    print("These events should trigger AI detection and appear in your AI events table.")
    print("\nTo clean up test data from BigQuery:")
    print("DELETE FROM `{project_id}.{dataset_id}.{table_id}` WHERE title = 'TEST_DATA_DELETE';")
    print("DELETE FROM `{project_id}.{dataset_id}.{table_id}_ai_events` WHERE title = 'TEST_DATA_DELETE';")

if __name__ == "__main__":
    try:
        publish_test_events()
    except Exception as e:
        print(f"Error publishing test events: {e}")
        sys.exit(1)
