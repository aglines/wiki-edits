#!/usr/bin/env python3
"""Test Pub/Sub publishing and subscription."""
import sys
import os
import json
from datetime import datetime

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from google.cloud import pubsub_v1
from wiki_pipeline.utils import validate_config, ConfigError, setup_logging, get_logger

def test_pubsub_publish():
    """Test publishing a message to Pub/Sub."""
    try:
        config = validate_config()
        publisher = pubsub_v1.PublisherClient()
        topic_path = config['topic']
        
        print(f"🔍 Testing Pub/Sub publishing to: {topic_path}")
        
        # Create test message
        test_message = {
            "meta": {"id": "test-123", "dt": datetime.now().isoformat(), "domain": "en.wikipedia.org"},
            "type": "edit",
            "title": "Test Article",
            "user": "TestUser",
            "wiki": "enwiki"
        }
        
        # Publish message
        message_data = json.dumps(test_message).encode('utf-8')
        future = publisher.publish(topic_path, message_data)
        message_id = future.result(timeout=10)
        
        print(f"✅ Successfully published test message with ID: {message_id}")
        return True
        
    except Exception as e:
        print(f"❌ Pub/Sub publish failed: {e}")
        return False

def test_pubsub_subscription():
    """Test if subscription can receive messages."""
    try:
        config = validate_config()
        subscriber = pubsub_v1.SubscriberClient()
        
        # Use the configured subscription path
        subscription_path = config['subscription']
        print(f"📡 Testing subscription: {subscription_path}")
        
        # Check if subscription exists
        try:
            subscriber.get_subscription(request={"subscription": subscription_path})
            print("✅ Subscription exists")
        except Exception as e:
            if "not found" in str(e).lower():
                print(f"⚠️  Subscription not found: {subscription_path}")
                topic_name = config['topic'].split('/')[-1]
                sub_name = config['subscription'].split('/')[-1]
                print("   Create it with:")
                print(f"   gcloud pubsub subscriptions create {sub_name} --topic={topic_name}")
                return False
            else:
                raise e
        
        # Test pulling messages from subscription
        print(f"🔍 Testing message pull from: {subscription_path}")
        
        # Pull messages (non-blocking)
        response = subscriber.pull(
            request={"subscription": subscription_path, "max_messages": 5},
            timeout=5.0
        )
        
        if response.received_messages:
            print(f"📨 Found {len(response.received_messages)} messages in subscription")
            for msg in response.received_messages:
                try:
                    data = json.loads(msg.message.data.decode('utf-8'))
                    print(f"   - {data.get('type', 'unknown')}: {data.get('title', 'N/A')[:50]}")
                except:
                    print(f"   - Raw message: {msg.message.data[:100]}...")
            
            # Acknowledge messages
            ack_ids = [msg.ack_id for msg in response.received_messages]
            subscriber.acknowledge(request={"subscription": subscription_path, "ack_ids": ack_ids})
            print("✅ Messages acknowledged")
            return True
        else:
            print("📭 No messages found in subscription (this is normal if stream isn't running)")
            return True
            
    except Exception as e:
        print(f"❌ Subscription test failed: {e}")
        return False

def main():
    """Test Pub/Sub setup."""
    setup_logging(level="INFO", structured=False)
    
    print("🧪 Testing Pub/Sub Setup")
    print("=" * 40)
    
    # Test publishing
    pub_success = test_pubsub_publish()
    
    print()
    
    # Test subscription  
    sub_success = test_pubsub_subscription()
    
    print()
    print("=" * 40)
    if pub_success and sub_success:
        print("✅ Pub/Sub setup looks good!")
        print("💡 If streaming still doesn't work, check the Wikimedia connection")
    else:
        print("❌ Pub/Sub setup has issues - fix these first")

if __name__ == '__main__':
    main()