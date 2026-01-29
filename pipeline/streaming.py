"""Wikimedia SSE streaming to Google Cloud Pub/Sub."""
import json
import logging
import time
from typing import Optional

import requests
from google.api_core import retry
from google.cloud import pubsub_v1

logger = logging.getLogger(__name__)


class WikimediaStreamer:
    """Handles streaming from Wikimedia SSE to Pub/Sub."""
    
    def __init__(self, project_id: str, topic_name: str):
        self.publisher = pubsub_v1.PublisherClient()
        self.topic_path = topic_name
        self.session = requests.Session()
        self.max_retries = 5
        self.base_delay = 1  # Base delay in seconds
        
    @retry.Retry(deadline=30)
    def _publish_event(self, data: dict) -> None:
        """Publish event to Pub/Sub with retry logic."""
        try:
            message_data = json.dumps(data).encode('utf-8')
            future = self.publisher.publish(self.topic_path, message_data)
            future.result(timeout=10)
        except Exception as e:
            logger.error(f"Failed to publish event: {e}")
            raise
    
    def _wait_with_backoff(self, attempt: int) -> None:
        """Wait with exponential backoff."""
        delay = self.base_delay * (2 ** attempt)
        max_delay = 60  # Cap at 1 minute
        delay = min(delay, max_delay)
        logger.info(f"Waiting {delay} seconds before retry attempt {attempt + 1}")
        time.sleep(delay)
    
    def stream_to_pubsub(self, max_events: Optional[int] = None) -> None:
        """Stream events from Wikimedia to Pub/Sub with retry logic."""
        url = 'https://stream.wikimedia.org/v2/stream/recentchange'
        headers = {
            'Accept': 'text/event-stream',
            'Cache-Control': 'no-cache',
            'User-Agent': 'WikiEditsPipeline/1.0 (aglines@example.com) Python/requests'
        }
        
        logger.info(f"Starting stream ingestion to {self.topic_path}")
        total_event_count = 0
        retry_count = 0
        
        while retry_count < self.max_retries:
            try:
                logger.info(f"Connecting to stream (attempt {retry_count + 1}/{self.max_retries})")
                response = self.session.get(url, headers=headers, stream=True, timeout=30)
                response.raise_for_status()
                
                # Reset retry count on successful connection
                if retry_count > 0:
                    logger.info("Successfully reconnected to stream")
                    retry_count = 0
                
                # Manual SSE parsing - more reliable than sseclient
                for line in response.iter_lines(decode_unicode=True):
                    if line.startswith('data: '):
                        event_data = line[6:]  # Remove 'data: ' prefix
                        if event_data:
                            try:
                                data = json.loads(event_data)
                                
                                # Skip canary events
                                if data.get('meta', {}).get('domain') == 'canary':
                                    continue
                                    
                                self._publish_event(data)
                                total_event_count += 1
                                
                                if total_event_count % 100 == 0:
                                    logger.info(f"Streamed {total_event_count} events")
                                    
                                if max_events and total_event_count >= max_events:
                                    logger.info(f"Reached max events: {max_events}")
                                    return
                                    
                            except json.JSONDecodeError as e:
                                logger.error(f"Failed to parse event: {e}")
                                continue
                            except Exception as e:
                                logger.error(f"Error processing event: {e}")
                                continue
                            
            except requests.RequestException as e:
                logger.error(f"Stream connection error: {e}")
                retry_count += 1
                
                if retry_count >= self.max_retries:
                    logger.error(f"Maximum retries ({self.max_retries}) exceeded. Giving up.")
                    raise
                
                self._wait_with_backoff(retry_count - 1)
                continue
                
            except KeyboardInterrupt:
                logger.info("Stream stopped by user")
                break
                
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                retry_count += 1
                
                if retry_count >= self.max_retries:
                    logger.error(f"Maximum retries ({self.max_retries}) exceeded. Giving up.")
                    raise
                
                self._wait_with_backoff(retry_count - 1)
                continue
        
        logger.info(f"Total events streamed: {total_event_count}")
