"""Utility modules: config, logging, validation, monitoring, timeouts, converters."""
import os
import logging
import sys
import signal
import time
import json
from typing import Dict, Any, List, Optional, Union
from datetime import datetime, timedelta
from dataclasses import dataclass

from dotenv import load_dotenv
from google.cloud import monitoring_v3
from google.cloud import pubsub_v1

# Load .env file for local development
load_dotenv()

logger = logging.getLogger(__name__)


# ============================================================================
# CONFIGURATION
# ============================================================================

class ConfigError(Exception):
    """Raised when configuration is invalid or missing."""
    pass


def validate_config() -> Dict[str, str]:
    """
    Validate all required configuration values from environment variables.

    Returns:
        Dictionary of validated configuration values

    Raises:
        ConfigError: If any required configuration is missing
    """
    required_vars = {
        'project_id': 'GCP_PROJECT_ID',
        'topic': 'PUBSUB_TOPIC',
        'subscription': 'PUBSUB_SUBSCRIPTION',
        'temp_location': 'GCS_TEMP_LOCATION',
        'region': 'GCP_REGION',
        'job_name': 'DATAFLOW_JOB_NAME',
        'output_table': 'BIGQUERY_OUTPUT_TABLE',
    }

    # Optional scaling configuration with defaults
    optional_vars = {
        'max_workers': ('DATAFLOW_MAX_WORKERS', '20'),
        'num_workers': ('DATAFLOW_NUM_WORKERS', '2'),
        'machine_type': ('DATAFLOW_MACHINE_TYPE', 'n1-standard-2'),
        'disk_size_gb': ('DATAFLOW_DISK_SIZE_GB', '50'),
        'enable_streaming_engine': ('DATAFLOW_STREAMING_ENGINE', 'true'),
    }

    config = {}
    missing = []

    # Validate required vars
    for key, env_var in required_vars.items():
        value = os.getenv(env_var, '').strip()
        if not value:
            missing.append(env_var)
        else:
            config[key] = value

    # Add optional vars with defaults
    for key, (env_var, default) in optional_vars.items():
        config[key] = os.getenv(env_var, default).strip()

    if missing:
        raise ConfigError(f"Missing required environment variables: {missing}")

    logger.info("Configuration validated successfully")
    logger.info(f"Scaling config: {config['num_workers']}-{config['max_workers']} workers, {config['machine_type']}")
    return config


# ============================================================================
# LOGGING
# ============================================================================

def setup_logging(level: str = "INFO", structured: bool = True) -> None:
    """Configure logging for the application."""
    log_level = getattr(logging, level.upper(), logging.INFO)
    
    if structured:
        formatter = logging.Formatter(
            '{"timestamp": "%(asctime)s", "name": "%(name)s", "level": "%(levelname)s", "message": "%(message)s"}',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
    else:
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
    
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    
    # Remove existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Add console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)
    
    # Set specific loggers for better debugging
    logging.getLogger('apache_beam').setLevel(logging.WARNING)
    logging.getLogger('google.cloud').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)


def get_logger(name: str) -> logging.Logger:
    """Get a logger instance for the given name."""
    return logging.getLogger(name)


# ============================================================================
# VALIDATION
# ============================================================================

@dataclass
class ValidationResult:
    """Result of data validation with quality metrics."""
    is_valid: bool
    data: Optional[Dict[str, Any]]
    errors: List[str]
    warnings: List[str]
    quality_score: float  # 0.0 to 1.0


class ValidationError(Exception):
    """Raised when data validation fails."""
    pass


def validate_required_fields(data: Dict[str, Any], required_fields: List[str]) -> None:
    """Validate that all required fields are present in data."""
    missing_fields = [field for field in required_fields if field not in data]
    if missing_fields:
        raise ValidationError(f"Missing required fields: {missing_fields}")


def validate_field_types(data: Dict[str, Any], field_types: Dict[str, type]) -> None:
    """Validate that fields have the expected types."""
    for field, expected_type in field_types.items():
        if field in data and not isinstance(data[field], expected_type):
            actual_type = type(data[field]).__name__
            expected_type_name = expected_type.__name__
            raise ValidationError(
                f"Field '{field}' has type {actual_type}, expected {expected_type_name}"
            )


def sanitize_string_field(value: Any, max_length: int = 1000) -> str:
    """Sanitize and validate string fields."""
    if value is None:
        return ""
    
    str_value = str(value)
    if len(str_value) > max_length:
        logger.warning(f"String field truncated from {len(str_value)} to {max_length} characters")
        str_value = str_value[:max_length]
    
    return str_value.strip()


def safe_cast(value: Any, target_type: type, default: Any = None) -> Any:
    """Safely cast value to target type with fallback."""
    if value is None:
        return default
    
    try:
        if target_type == bool:
            if isinstance(value, str):
                return value.lower() in ('true', '1', 'yes')
            return bool(value)
        elif target_type == int:
            return int(float(value))  # Handle "123.0" strings
        elif target_type == float:
            return float(value)
        elif target_type == str:
            return str(value)
        else:
            return target_type(value)
    except (ValueError, TypeError):
        logger.warning(f"Failed to cast {value} to {target_type.__name__}, using default {default}")
        return default


def validate_extracted_data_robust(data: Dict[str, Any]) -> ValidationResult:
    """Professional-grade validation with graceful degradation."""
    errors = []
    warnings = []
    quality_score = 1.0
    
    if not data:
        return ValidationResult(False, None, ["Data cannot be empty"], [], 0.0)
    
    # Create a copy to avoid mutating original
    validated_data = {}
    
    # Define schema with defaults and validation rules
    schema = {
        # Required fields
        'evt_id': {'type': str, 'required': True, 'default': ''},
        'type': {'type': str, 'required': True, 'default': 'unknown'},
        'domain': {'type': str, 'required': True, 'default': 'unknown'},
        
        # Core fields with defaults
        'title': {'type': str, 'required': False, 'default': '', 'max_length': 500},
        'user': {'type': str, 'required': False, 'default': '', 'max_length': 100},
        'bot': {'type': bool, 'required': False, 'default': False},
        'ns': {'type': int, 'required': False, 'default': 0},
        'wiki': {'type': str, 'required': False, 'default': ''},
        'srv_name': {'type': str, 'required': False, 'default': ''},
        'srv_url': {'type': str, 'required': False, 'default': ''},
        
        # Edit fields
        'minor': {'type': bool, 'required': False, 'default': False},
        'patrolled': {'type': bool, 'required': False, 'default': False},
        'len_old': {'type': int, 'required': False, 'default': 0},
        'len_new': {'type': int, 'required': False, 'default': 0},
        'rev_old': {'type': int, 'required': False, 'default': 0},
        'rev_new': {'type': int, 'required': False, 'default': 0},
        'comment': {'type': str, 'required': False, 'default': '', 'max_length': 1000},
        'parsed_comment': {'type': str, 'required': False, 'default': '', 'max_length': 1000},
        
        # Log fields
        'log_type': {'type': str, 'required': False, 'default': ''},
        'log_action': {'type': str, 'required': False, 'default': ''},
        'log_id': {'type': int, 'required': False, 'default': 0},
        'log_comment': {'type': str, 'required': False, 'default': '', 'max_length': 1000},
        
        # Datetime fields (special handling)
        'dt_user': {'type': str, 'required': False, 'default': None},
        'dt_server': {'type': str, 'required': False, 'default': None},
    }
    
    # Validate each field according to schema
    for field_name, field_config in schema.items():
        raw_value = data.get(field_name)
        
        # Handle required fields
        if field_config['required'] and (raw_value is None or raw_value == ''):
            errors.append(f"Required field '{field_name}' is missing or empty")
            quality_score -= 0.3
            validated_data[field_name] = field_config['default']
            continue
        
        # Cast to correct type with fallback
        validated_value = safe_cast(raw_value, field_config['type'], field_config['default'])
        
        # Handle string length limits
        if field_config['type'] == str and 'max_length' in field_config:
            max_len = field_config['max_length']
            if validated_value and len(validated_value) > max_len:
                warnings.append(f"Field '{field_name}' truncated from {len(validated_value)} to {max_len} chars")
                validated_value = validated_value[:max_len]
                quality_score -= 0.05
        
        # Sanitize strings
        if field_config['type'] == str and validated_value:
            validated_value = validated_value.strip()
        
        validated_data[field_name] = validated_value
    
    # Handle unknown fields (schema drift)
    unknown_fields = set(data.keys()) - set(schema.keys())
    if unknown_fields:
        warnings.append(f"Unknown fields detected (schema drift): {list(unknown_fields)}")
        quality_score -= 0.1
        logger.info(f"Schema drift detected: new fields {unknown_fields}")
    
    # Determine if data is valid (has required fields)
    is_valid = len(errors) == 0 or all('Required field' not in error for error in errors)
    
    return ValidationResult(
        is_valid=is_valid,
        data=validated_data if is_valid else None,
        errors=errors,
        warnings=warnings,
        quality_score=max(0.0, quality_score)
    )


# ============================================================================
# CONVERTERS
# ============================================================================

def clean_boolean_field(msg: Dict[str, Any], key: str) -> Dict[str, Any]:
    """Clean boolean fields by ensuring they have proper boolean values."""
    if key in msg:
        msg[key] = bool(msg[key])
    return msg


# ============================================================================
# TIMEOUT
# ============================================================================

class TimeoutError(Exception):
    """Raised when a timeout occurs."""
    pass


class TimeoutContext:
    """Context manager for setting execution time limits using signals."""
    
    def __init__(self, hours: Optional[int] = None):
        """Initialize timeout context."""
        self.hours = hours
        self.seconds = hours * 3600 if hours else None
        self.old_handler = None
        
    def _timeout_handler(self, signum, frame):
        """Signal handler for timeout."""
        raise TimeoutError(f"Execution exceeded {self.hours} hour(s)")
    
    def __enter__(self):
        """Set up the timeout signal."""
        if self.seconds is not None:
            logger.info(f"⏱️  Setting execution timeout: {self.hours} hour(s) ({self.seconds}s)")
            self.old_handler = signal.signal(signal.SIGALRM, self._timeout_handler)
            signal.alarm(self.seconds)
        else:
            logger.info("⏱️  No timeout set - will run until manually stopped")
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Cancel the timeout signal."""
        if self.seconds is not None:
            signal.alarm(0)  # Cancel the alarm
            if self.old_handler is not None:
                signal.signal(signal.SIGALRM, self.old_handler)
        
        # If timeout occurred, log it but don't suppress the exception
        if exc_type is TimeoutError:
            logger.warning(f"⏱️  Timeout reached after {self.hours} hour(s)")
            return False  # Re-raise the exception
        
        return False  # Don't suppress other exceptions


# ============================================================================
# PIPELINE MONITORING
# ============================================================================

class PipelineMonitor:
    """Monitor Dataflow pipeline performance and suggest scaling adjustments."""
    
    def __init__(self, config: dict):
        self.config = config
        self.project_id = config['project_id']
        self.region = config['region']
        self.subscription_name = config['subscription']
        
        # Initialize clients
        self.monitoring_client = monitoring_v3.MetricServiceClient()
        self.pubsub_client = pubsub_v1.SubscriberClient()
        
    def get_subscription_backlog(self) -> int:
        """Get current Pub/Sub subscription backlog."""
        try:
            subscription_path = self.pubsub_client.subscription_path(
                self.project_id, self.subscription_name.split('/')[-1]
            )
            
            # Query for backlog metrics
            project_name = f"projects/{self.project_id}"
            interval = monitoring_v3.TimeInterval({
                "end_time": {"seconds": int(time.time())},
                "start_time": {"seconds": int(time.time() - 300)},  # Last 5 minutes
            })
            
            results = self.monitoring_client.list_time_series(
                request={
                    "name": project_name,
                    "filter": f'metric.type="pubsub.googleapis.com/subscription/num_undelivered_messages" '
                             f'AND resource.labels.subscription_id="{self.subscription_name.split("/")[-1]}"',
                    "interval": interval,
                    "view": monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL,
                }
            )
            
            latest_value = 0
            for result in results:
                if result.points:
                    latest_value = result.points[0].value.int64_value
                    break
                    
            return latest_value
            
        except Exception as e:
            logger.error(f"Error getting subscription backlog: {e}")
            return 0
    
    def get_active_jobs(self) -> List[Dict]:
        """Get active Dataflow jobs - simplified without dataflow client."""
        logger.info("Dataflow job monitoring disabled - check GCP Console manually")
        return []
    
    def analyze_performance(self) -> Dict:
        """Analyze current pipeline performance and suggest improvements."""
        backlog = self.get_subscription_backlog()
        active_jobs = self.get_active_jobs()
        
        analysis = {
            'timestamp': datetime.now().isoformat(),
            'backlog_messages': backlog,
            'active_jobs': len(active_jobs),
            'recommendations': []
        }
        
        # Analyze backlog and suggest scaling
        if backlog > 10000:
            analysis['recommendations'].append({
                'severity': 'HIGH',
                'issue': 'High backlog detected',
                'suggestion': f'Consider increasing max_workers to 30-50. Current backlog: {backlog:,} messages',
                'env_vars': {
                    'DATAFLOW_MAX_WORKERS': '20',
                    'DATAFLOW_NUM_WORKERS': '5'
                }
            })
        elif backlog > 5000:
            analysis['recommendations'].append({
                'severity': 'MEDIUM',
                'issue': 'Moderate backlog detected',
                'suggestion': f'Consider increasing max_workers to 25. Current backlog: {backlog:,} messages',
                'env_vars': {
                    'DATAFLOW_MAX_WORKERS': '10',
                    'DATAFLOW_NUM_WORKERS': '3'
                }
            })
        elif backlog < 100 and len(active_jobs) > 0:
            analysis['recommendations'].append({
                'severity': 'LOW',
                'issue': 'Low backlog - potential over-provisioning',
                'suggestion': 'Consider reducing max_workers to save costs',
                'env_vars': {
                    'DATAFLOW_MAX_WORKERS': '5',
                    'DATAFLOW_NUM_WORKERS': '1'
                }
            })
        
        # Check for job failures or issues
        for job in active_jobs:
            if job['state'] == 'JOB_STATE_PENDING':
                analysis['recommendations'].append({
                    'severity': 'MEDIUM',
                    'issue': f'Job {job["name"]} stuck in pending state',
                    'suggestion': 'Check quotas, permissions, or resource availability'
                })
        
        return analysis
    
    def print_status(self):
        """Print current pipeline status."""
        analysis = self.analyze_performance()
        
        print(f"\n📊 Pipeline Status - {analysis['timestamp']}")
        print(f"📨 Pub/Sub Backlog: {analysis['backlog_messages']:,} messages")
        print(f"🔄 Active Jobs: {analysis['active_jobs']}")
        
        if analysis['recommendations']:
            print(f"\n💡 Recommendations:")
            for rec in analysis['recommendations']:
                severity_emoji = {'HIGH': '🚨', 'MEDIUM': '⚠️', 'LOW': '💡'}
                print(f"{severity_emoji.get(rec['severity'], '📝')} {rec['severity']}: {rec['issue']}")
                print(f"   Suggestion: {rec['suggestion']}")
                if 'env_vars' in rec:
                    print(f"   Environment variables:")
                    for key, value in rec['env_vars'].items():
                        print(f"     {key}={value}")
                print()
        else:
            print("✅ No issues detected - pipeline running optimally")
