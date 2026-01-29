"""Field extraction utilities for Wikipedia edit events."""
import logging
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)


def extract_core_fields(msg: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract core fields that are present in all event types.
    
    Args:
        msg: Raw message dictionary
        
    Returns:
        Dictionary with core fields
        
    Raises:
        KeyError: If required fields are missing
    """
    return {
        'evt_id': msg['meta']['id'],
        'type': msg['type'],
        'title': msg.get('title', ''),
        'user': msg.get('user', ''),
        'bot': msg.get('bot', False),
        'dt_user': msg['meta']['dt'],
        'dt_server': msg.get('timestamp', 0),
        'domain': msg['meta']['domain'],
        'ns': msg.get('namespace', 0),
        'wiki': msg.get('wiki', ''),
        'srv_name': msg.get('server_name', ''),
        'srv_url': msg.get('server_url', ''),
    }


def extract_edit_fields(msg: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract fields specific to edit and new page events.
    
    Args:
        msg: Raw message dictionary
        
    Returns:
        Dictionary with edit-specific fields
    """
    return {
        'minor': msg.get('minor', False),
        'patrolled': msg.get('patrolled', False),
        'len_old': msg.get('length', {}).get('old', 0),
        'len_new': msg.get('length', {}).get('new', 0),
        'rev_old': msg.get('revision', {}).get('old', 0),
        'rev_new': msg.get('revision', {}).get('new', 0),
        'comment': msg.get('comment', ''),
        'parsed_comment': msg.get('parsedcomment', ''),
    }


def extract_ai_content_fields(msg: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract content fields relevant for AI writing detection.
    Only uses confirmed fields that exist in Wikipedia edit messages.

    Args:
        msg: Raw message dictionary

    Returns:
        Dictionary with AI detection relevant fields
    """
    return {
        'comment': msg.get('comment', ''),
        'parsed_comment': msg.get('parsedcomment', ''),
        'title': msg.get('title', ''),
        'user': msg.get('user', ''),
        'len_old': msg.get('length', {}).get('old', 0),
        'len_new': msg.get('length', {}).get('new', 0),
        'minor': msg.get('minor', False),
        'bot': msg.get('bot', False),
    }


def extract_log_fields(msg: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract fields specific to log events.
    
    Args:
        msg: Raw message dictionary
        
    Returns:
        Dictionary with log-specific fields
    """
    return {
        'log_type': msg.get('log_type', ''),
        'log_action': msg.get('log_action', ''),
        'log_id': msg.get('log_id', 0),
        'log_comment': msg.get('log_action_comment', ''),
    }


def get_all_fields(msg: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Extract all available fields from any event type.
    
    Args:
        msg: Raw message dictionary
        
    Returns:
        Dictionary with extracted fields, or None if extraction fails
    """
    try:
        # Start with core fields
        data = extract_core_fields(msg)
        
        # Add type-specific fields
        event_type = msg.get('type', '')
        
        if event_type in ['edit', 'new']:
            data.update(extract_edit_fields(msg))
        elif event_type == 'log':
            data.update(extract_log_fields(msg))
        
        return data
        
    except (KeyError, TypeError, AttributeError) as e:
        logger.error(f"Failed to extract fields from message: {e}. Message type: {msg.get('type', 'unknown')}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error in get_all_fields: {e}")
        return None


