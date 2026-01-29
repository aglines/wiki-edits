"""BigQuery schema definition for Wikipedia edit events."""
import logging
from typing import Dict, Any, Set, List, Optional
from dataclasses import dataclass
from datetime import datetime

logger = logging.getLogger(__name__)

# Current BigQuery schema - string format for BigQuery compatibility
BQ_SCHEMA = """
evt_id:STRING,
type:STRING,
title:STRING,
user:STRING,
bot:BOOL,
dt_user:STRING,
dt_server:TIMESTAMP,
domain:STRING,
ns:INT64,
wiki:STRING,
srv_name:STRING,
srv_url:STRING,
minor:BOOL,
patrolled:BOOL,
len_old:INT64,
len_new:INT64,
rev_old:INT64,
rev_new:INT64,
comment:STRING,
parsed_comment:STRING,
log_type:STRING,
log_action:STRING,
log_id:INT64,
log_comment:STRING
"""

# Dead letter queue schema for invalid records
DLQ_SCHEMA = """
evt_id:STRING,
type:STRING,
title:STRING,
user:STRING,
bot:BOOL,
dt_user:STRING,
dt_server:TIMESTAMP,
domain:STRING,
ns:INT64,
wiki:STRING,
srv_name:STRING,
srv_url:STRING,
minor:BOOL,
patrolled:BOOL,
len_old:INT64,
len_new:INT64,
rev_old:INT64,
rev_new:INT64,
comment:STRING,
parsed_comment:STRING,
log_type:STRING,
log_action:STRING,
log_id:INT64,
log_comment:STRING,
_validation_timestamp:TIMESTAMP,
_error_category:STRING,
_error_details:STRING,
_original_payload:STRING,
_schema_version:STRING
"""

# AI events schema for tracking potential AI-generated content
AI_SCHEMA = """
event_id:STRING,
rule_version:STRING,
detection_timestamp:TIMESTAMP,
timestamp:STRING,
user:STRING,
title:STRING,
domain:STRING,
namespace:INT64,
edit_type:STRING,
ai_flags:JSON,
content_sample:JSON,
detection_metadata:JSON
"""

# BigQuery CREATE TABLE statement for AI events table
# Note: Table name should be constructed from environment variables
AI_EVENTS_CREATE_TABLE_TEMPLATE = """
CREATE TABLE `{project_id}.{dataset_id}.{table_id}_ai_events` (
  event_id STRING,
  rule_version STRING,
  detection_timestamp TIMESTAMP,
  timestamp STRING,
  user STRING,
  title STRING,
  domain STRING,
  namespace INT64,
  edit_type STRING,
  ai_flags JSON,
  content_sample JSON,
  detection_metadata JSON
)
PARTITION BY DATE(detection_timestamp)
CLUSTER BY rule_version, domain
"""

# Schema field definitions for validation
SCHEMA_FIELDS = {
    'evt_id': {'type': 'STRING', 'required': True, 'nullable': False},
    'type': {'type': 'STRING', 'required': True, 'nullable': False},
    'domain': {'type': 'STRING', 'required': True, 'nullable': False},
    'title': {'type': 'STRING', 'required': False, 'nullable': True},
    'user': {'type': 'STRING', 'required': False, 'nullable': True},
    'bot': {'type': 'BOOL', 'required': False, 'nullable': True},
    'dt_user': {'type': 'STRING', 'required': False, 'nullable': True},
    'dt_server': {'type': 'TIMESTAMP', 'required': False, 'nullable': True},
    'ns': {'type': 'INT64', 'required': False, 'nullable': True},
    'wiki': {'type': 'STRING', 'required': False, 'nullable': True},
    'srv_name': {'type': 'STRING', 'required': False, 'nullable': True},
    'srv_url': {'type': 'STRING', 'required': False, 'nullable': True},
    'minor': {'type': 'BOOL', 'required': False, 'nullable': True},
    'patrolled': {'type': 'BOOL', 'required': False, 'nullable': True},
    'len_old': {'type': 'INT64', 'required': False, 'nullable': True},
    'len_new': {'type': 'INT64', 'required': False, 'nullable': True},
    'rev_old': {'type': 'INT64', 'required': False, 'nullable': True},
    'rev_new': {'type': 'INT64', 'required': False, 'nullable': True},
    'comment': {'type': 'STRING', 'required': False, 'nullable': True},
    'parsed_comment': {'type': 'STRING', 'required': False, 'nullable': True},
    'log_type': {'type': 'STRING', 'required': False, 'nullable': True},
    'log_action': {'type': 'STRING', 'required': False, 'nullable': True},
    'log_id': {'type': 'INT64', 'required': False, 'nullable': True},
    'log_comment': {'type': 'STRING', 'required': False, 'nullable': True},
}

# Required fields for raw messages
REQUIRED_MESSAGE_FIELDS = {
    'meta': {'id', 'dt', 'domain'}
}

# Valid event types
VALID_EVENT_TYPES = {'edit', 'new', 'log', 'categorize', 'move'}

# Schema version for evolution tracking
SCHEMA_VERSION = "1.0.0"

@dataclass
class ValidationError:
    """Structured validation error for categorization."""
    category: str
    field: str
    expected_type: str
    actual_value: Any
    message: str
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'category': self.category,
            'field': self.field,
            'expected_type': self.expected_type,
            'actual_value': str(self.actual_value),
            'message': self.message
        }

class SchemaValidator:
    """Validates data against BigQuery schema with error categorization."""
    
    def __init__(self):
        self.schema_fields = SCHEMA_FIELDS
        self.schema_version = SCHEMA_VERSION
    
    def validate_record(self, record: Dict[str, Any]) -> tuple[bool, List[ValidationError]]:
        """
        Validate a record against the schema.
        
        Returns:
            Tuple of (is_valid, list_of_errors)
        """
        errors = []
        
        # Check required fields
        for field, config in self.schema_fields.items():
            if config['required'] and field not in record:
                errors.append(ValidationError(
                    category='MISSING_REQUIRED_FIELD',
                    field=field,
                    expected_type=str(config['type']),
                    actual_value=None,
                    message=f'Required field {field} is missing'
                ))
        
        # Check field types and constraints
        for field, value in record.items():
            if field in self.schema_fields:
                field_config = self.schema_fields[field]
                validation_error = self._validate_field_type(field, value, field_config)
                if validation_error:
                    errors.append(validation_error)
        
        return len(errors) == 0, errors
    
    def _validate_field_type(self, field: str, value: Any, config: Dict[str, Any]) -> Optional[ValidationError]:
        """Validate individual field type."""
        if value is None and config.get('nullable', True):
            return None
            
        expected_type = config['type']
        
        try:
            if expected_type == 'STRING':
                if not isinstance(value, str):
                    return ValidationError(
                        category='TYPE_MISMATCH',
                        field=field,
                        expected_type='STRING',
                        actual_value=value,
                        message=f'Expected string for {field}, got {type(value).__name__}'
                    )
            elif expected_type == 'INT64':
                if not isinstance(value, int):
                    return ValidationError(
                        category='TYPE_MISMATCH',
                        field=field,
                        expected_type='INT64',
                        actual_value=value,
                        message=f'Expected integer for {field}, got {type(value).__name__}'
                    )
            elif expected_type == 'BOOL':
                if not isinstance(value, bool):
                    return ValidationError(
                        category='TYPE_MISMATCH',
                        field=field,
                        expected_type='BOOL',
                        actual_value=value,
                        message=f'Expected boolean for {field}, got {type(value).__name__}'
                    )
            elif expected_type == 'TIMESTAMP':
                # Timestamps can be int (Unix timestamp) or string (ISO format)
                if not isinstance(value, (int, str)):
                    return ValidationError(
                        category='TYPE_MISMATCH',
                        field=field,
                        expected_type='TIMESTAMP',
                        actual_value=value,
                        message=f'Expected timestamp (int/string) for {field}, got {type(value).__name__}'
                    )
                    
        except Exception as e:
            return ValidationError(
                category='VALIDATION_ERROR',
                field=field,
                expected_type=expected_type,
                actual_value=value,
                message=f'Validation failed for {field}: {str(e)}'
            )
        
        return None
    
    def create_dlq_record(self, original_record: Dict[str, Any], errors: List[ValidationError]) -> Dict[str, Any]:
        """Create a dead letter queue record with structured error information."""
        # Start with original record, filling missing required fields with defaults
        dlq_record = original_record.copy()
        
        # Add required fields with defaults if missing
        for field, config in self.schema_fields.items():
            if field not in dlq_record:
                if config['type'] == 'STRING':
                    dlq_record[field] = ''
                elif config['type'] == 'INT64':
                    dlq_record[field] = 0
                elif config['type'] == 'BOOL':
                    dlq_record[field] = False
                elif config['type'] == 'TIMESTAMP':
                    dlq_record[field] = 0
        
        # Add validation metadata
        dlq_record.update({
            '_validation_timestamp': datetime.utcnow(),
            '_error_category': self._categorize_errors(errors),
            '_error_details': self._serialize_errors(errors),
            '_original_payload': str(original_record),
            '_schema_version': self.schema_version
        })
        
        return dlq_record
    
    def _categorize_errors(self, errors: List[ValidationError]) -> str:
        """Categorize errors for monitoring and alerting."""
        if not errors:
            return 'VALID'
        
        categories = [error.category for error in errors]
        
        if 'MISSING_REQUIRED_FIELD' in categories:
            return 'MISSING_REQUIRED_FIELD'
        elif 'TYPE_MISMATCH' in categories:
            return 'TYPE_MISMATCH'
        elif 'VALIDATION_ERROR' in categories:
            return 'VALIDATION_ERROR'
        else:
            return 'OTHER'
    
    def _serialize_errors(self, errors: List[ValidationError]) -> str:
        """Serialize errors to JSON string for storage."""
        return str([error.to_dict() for error in errors])

# Global validator instance
schema_validator = SchemaValidator()


def validate_message_structure(msg: dict) -> bool:
    """
    Validate that a message has the required structure.
    
    Args:
        msg: Message dictionary to validate
        
    Returns:
        True if message structure is valid, False otherwise
    """
    try:
        # Check required top-level fields
        if not isinstance(msg.get('meta'), dict):
            return False
        
        # Check required meta fields
        meta = msg['meta']
        for field in REQUIRED_MESSAGE_FIELDS['meta']:
            if field not in meta:
                return False
        
        # Check event type
        event_type = msg.get('type')
        if not isinstance(event_type, str) or event_type not in VALID_EVENT_TYPES:
            return False
        
        return True
        
    except (KeyError, TypeError, AttributeError):
        return False
