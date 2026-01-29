#!/usr/bin/env python3
"""Connectivity tests for Wikimedia stream and Pub/Sub."""
import json
import pytest
from datetime import datetime

import requests
from google.cloud import pubsub_v1
from wiki_pipeline.utils import validate_config, ConfigError


@pytest.fixture(scope="module")
def config():
    """Load and validate configuration once for all tests."""
    return validate_config()


@pytest.fixture(scope="module")
def publisher():
    """Create Pub/Sub publisher client."""
    return pubsub_v1.PublisherClient()


@pytest.fixture(scope="module")
def subscriber():
    """Create Pub/Sub subscriber client."""
    return pubsub_v1.SubscriberClient()


class TestWikimediaStream:
    """Test Wikimedia SSE stream connection."""

    def test_stream_connection(self):
        """Test if we can connect to and receive data from Wikimedia SSE."""
        url = 'https://stream.wikimedia.org/v2/stream/recentchange'
        headers = {
            'Accept': 'text/event-stream',
            'Cache-Control': 'no-cache',
            'User-Agent': 'WikiEditsPipeline/1.0 (test@example.com) Python/requests'
        }

        response = requests.get(url, headers=headers, stream=True, timeout=30)
        assert response.status_code == 200, "Failed to connect to Wikimedia stream"

        event_count = 0
        max_events = 5

        for line in response.iter_lines(decode_unicode=True):
            if line.startswith('data: '):
                event_data = line[6:]
                if event_data:
                    data = json.loads(event_data)
                    if data.get('meta', {}).get('domain') != 'canary':
                        event_count += 1
                        assert 'type' in data, "Event missing 'type' field"
                        assert 'meta' in data, "Event missing 'meta' field"
                        if event_count >= max_events:
                            break

        assert event_count > 0, "No valid events received from Wikimedia stream"


class TestPubSubPublish:
    """Test Pub/Sub publishing functionality."""

    def test_publish_message(self, config, publisher):
        """Test publishing a message to Pub/Sub."""
        topic_path = config['topic']
        test_message = {
            "meta": {"id": "test-123", "dt": datetime.now().isoformat(), "domain": "en.wikipedia.org"},
            "type": "edit",
            "title": "Test Article",
            "user": "TestUser",
            "wiki": "enwiki"
        }

        message_data = json.dumps(test_message).encode('utf-8')
        future = publisher.publish(topic_path, message_data)
        message_id = future.result(timeout=10)

        assert message_id is not None, "Failed to get message ID from publish"


class TestPubSubSubscription:
    """Test Pub/Sub subscription functionality."""

    def test_subscription_exists(self, config, subscriber):
        """Test if subscription exists and is accessible."""
        subscription_path = config['subscription']
        subscription = subscriber.get_subscription(request={"subscription": subscription_path})
        assert subscription is not None, f"Subscription not found: {subscription_path}"

    def test_pull_messages(self, config, subscriber):
        """Test pulling messages from subscription (non-blocking check)."""
        subscription_path = config['subscription']
        response = subscriber.pull(
            request={"subscription": subscription_path, "max_messages": 5},
            timeout=5.0
        )

        # If messages exist, verify they're valid
        if response.received_messages:
            for msg in response.received_messages:
                assert msg.message.data, "Message has no data"
                # Try to parse as JSON
                data = json.loads(msg.message.data.decode('utf-8'))
                assert 'meta' in data or 'type' in data, "Message missing expected fields"

            # Acknowledge messages to clean up
            ack_ids = [msg.ack_id for msg in response.received_messages]
            subscriber.acknowledge(request={"subscription": subscription_path, "ack_ids": ack_ids})


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
