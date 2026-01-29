"""Event filtering utilities for Wikipedia edit events."""
import json
import logging
from typing import Any, Dict, Generator

import apache_beam as beam
from apache_beam.pvalue import TaggedOutput
from ..schema import validate_message_structure
from .extractors import extract_ai_content_fields

logger = logging.getLogger(__name__)


class AllEvents(beam.DoFn):
    """Filter to process all event types and exclude canary events."""
    
    def process(self, element) -> Generator[dict, None, None]:
        """
        Process all event types, filter canary events.
        
        Args:
            element: Pub/Sub message element
            
        Yields:
            Parsed message dictionaries (excluding canary events)
        """
        try:
            msg = json.loads(element.data.decode('utf-8'))
            # Validate message structure before processing
            if validate_message_structure(msg) and msg.get('meta', {}).get('domain') != 'canary':
                yield msg
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            logger.error(f"Failed to decode message: {e}")
        except AttributeError as e:
            logger.error(f"Message missing expected attributes: {e}")
        except Exception as e:
            logger.error(f"Unexpected error in AllEvents filter: {e}")


class MediaFilter(beam.DoFn):
    """Filter to handle media files while preserving metadata."""
    
    def process(self, element) -> Generator[dict, None, None]:
        """
        Filter media files but keep metadata.
        
        Args:
            element: Message dictionary
            
        Yields:
            Filtered message dictionaries
        """
        try:
            ns = element.get('namespace', 0)
            # Namespace 6 = File/Media namespace
            if ns == 6:
                # Keep metadata, remove content-heavy fields
                filtered = element.copy()
                filtered.pop('log_params', None)
                yield filtered
            else:
                yield element
        except Exception as e:
            logger.error(f"Unexpected error in MediaFilter: {e}")
            # Yield original element on error to avoid data loss
            yield element


class AIContentMonitor(beam.DoFn):
    """Monitor edits for potential AI-generated content indicators."""
    
    def process(self, element):
        """
        Analyze content for AI writing indicators.

        Args:
            element: Message dictionary

        Yields:
            Original element with ai_flags added if indicators found
            Side output: AI events for tracking/monitoring
        """
        try:
            content = extract_ai_content_fields(element)
        except (ImportError, AttributeError) as e:
            logger.warning(f"AI content extraction not available: {e}")
            yield element
            return
        except Exception as e:
            logger.error(f"Failed to extract AI content fields: {e}")
            yield element
            return

        try:
            ai_flags = AIContentMonitor._detect_ai_indicators_static(content)
            if ai_flags:
                element['ai_flags'] = ai_flags

                # Create AI event for side output tracking
                ai_event = self._create_ai_event(element, ai_flags, content)
                yield TaggedOutput('ai_events', ai_event)

        except Exception as e:
            logger.error(f"Error in AI indicator detection: {e}")

        yield element

    def _create_ai_event(self, element: dict, ai_flags: dict, content: dict) -> dict:
        """Create AI event record for tracking."""
        from ..analysis.ai_detection import DETECTION_RULE_VERSION
        from datetime import datetime
        
        return {
            'event_id': element.get('evt_id', ''),
            'rule_version': DETECTION_RULE_VERSION,
            'detection_timestamp': datetime.utcnow(),
            'timestamp': element.get('dt_user', ''),
            'user': element.get('user', ''),
            'title': element.get('title', ''),
            'domain': element.get('domain', ''),
            'namespace': element.get('ns', 0),
            'edit_type': element.get('type', ''),
            'ai_flags': ai_flags,
            'content_sample': {
                'comment': content.get('comment', '')[:500],  # First 500 chars
                'title': content.get('title', ''),
                'text_length': len(content.get('text', ''))
            },
            'detection_metadata': {
                'total_flags': len(ai_flags),
                'flag_types': list(ai_flags.keys()),
                'detected_at': element.get('dt_server', 0),
                'rule_version': DETECTION_RULE_VERSION
            }
        }

    @staticmethod
    def _detect_ai_indicators_static(content: Dict[str, Any]) -> Dict[str, Any]:
        """Detect AI writing indicators in content. Logic moved to separate file."""
        from ..analysis.ai_detection import detect_ai_indicators
        return detect_ai_indicators(content)
