#!/usr/bin/env python3
"""Setup script to create Pub/Sub topic and subscription."""
from google.cloud import pubsub_v1
from google.api_core.exceptions import AlreadyExists, NotFound
from wiki_pipeline.utils import validate_config, ConfigError, setup_logging, get_logger

def create_topic(publisher, topic_path):
    """Create Pub/Sub topic if it doesn't exist."""
    try:
        topic = publisher.create_topic(request={"name": topic_path})
        logger.info(f"✅ Created topic: {topic.name}")
        return True
    except AlreadyExists:
        logger.info(f"ℹ️  Topic already exists: {topic_path}")
        return True
    except Exception as e:
        logger.error(f"❌ Failed to create topic: {e}")
        return False

def create_subscription(subscriber, subscription_path, topic_path):
    """Create Pub/Sub subscription if it doesn't exist."""
    try:
        subscription = subscriber.create_subscription(
            request={"name": subscription_path, "topic": topic_path}
        )
        logger.info(f"✅ Created subscription: {subscription.name}")
        return True
    except AlreadyExists:
        logger.info(f"ℹ️  Subscription already exists: {subscription_path}")
        return True
    except Exception as e:
        logger.error(f"❌ Failed to create subscription: {e}")
        return False

def main():
    """Set up Pub/Sub topic and subscription."""
    setup_logging(level="INFO", structured=False)
    global logger
    logger = get_logger(__name__)
    
    try:
        # Load config
        config = validate_config()
        logger.info("🔧 Setting up Pub/Sub resources...")
        
        # Initialize clients
        publisher = pubsub_v1.PublisherClient()
        subscriber = pubsub_v1.SubscriberClient()
        
        topic_path = config['topic']
        subscription_path = config['subscription']
        
        logger.info(f"Topic: {topic_path}")
        logger.info(f"Subscription: {subscription_path}")
        
        # Create topic
        topic_success = create_topic(publisher, topic_path)
        if not topic_success:
            logger.error("Failed to create topic, aborting")
            sys.exit(1)
        
        # Create subscription
        sub_success = create_subscription(subscriber, subscription_path, topic_path)
        if not sub_success:
            logger.error("Failed to create subscription, aborting")
            sys.exit(1)
        
        logger.info("🎉 Pub/Sub setup complete!")
        logger.info("")
        logger.info("📋 Next steps:")
        logger.info("1. Test streaming: python main.py stream --max-events 10")
        logger.info("2. Test pipeline: python main.py pipeline")
        logger.info("3. Monitor in GCP Console: Cloud Pub/Sub > Topics")
        
    except ConfigError as e:
        logger.error(f"Configuration error: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Setup failed: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()