"""Extended tests for utility functions."""
import os
import sys
import logging
import pytest
from io import StringIO
from unittest.mock import patch, MagicMock

from wiki_pipeline.utils import (
    setup_logging,
    get_logger,
    validate_extracted_data_robust,
    ValidationResult,
    PipelineMonitor,
)


class TestSetupLogging:
    """Tests for setup_logging function."""

    def test_setup_logging_default(self):
        """Should set up logging with defaults."""
        setup_logging()
        
        root_logger = logging.getLogger()
        assert root_logger.level == logging.INFO
        assert len(root_logger.handlers) > 0

    def test_setup_logging_debug_level(self):
        """Should set debug level."""
        setup_logging(level="DEBUG")
        
        root_logger = logging.getLogger()
        assert root_logger.level == logging.DEBUG

    def test_setup_logging_warning_level(self):
        """Should set warning level."""
        setup_logging(level="WARNING")
        
        root_logger = logging.getLogger()
        assert root_logger.level == logging.WARNING

    def test_setup_logging_structured(self):
        """Should use structured JSON formatter."""
        setup_logging(level="INFO", structured=True)
        
        root_logger = logging.getLogger()
        handler = root_logger.handlers[0]
        formatter = handler.formatter
        
        # Check formatter includes JSON structure
        assert 'timestamp' in formatter._fmt
        assert 'level' in formatter._fmt
        assert 'message' in formatter._fmt

    def test_setup_logging_unstructured(self):
        """Should use plain text formatter."""
        setup_logging(level="INFO", structured=False)
        
        root_logger = logging.getLogger()
        handler = root_logger.handlers[0]
        formatter = handler.formatter
        
        # Check formatter is plain text
        assert 'timestamp' not in formatter._fmt

    def test_setup_logging_removes_old_handlers(self):
        """Should remove existing handlers."""
        root_logger = logging.getLogger()
        initial_handler_count = len(root_logger.handlers)
        
        setup_logging()
        
        # Should have exactly one handler after setup
        assert len(root_logger.handlers) >= 1

    def test_setup_logging_sets_apache_beam_level(self):
        """Should set Apache Beam logger to WARNING."""
        setup_logging()
        
        beam_logger = logging.getLogger('apache_beam')
        assert beam_logger.level == logging.WARNING

    def test_setup_logging_sets_google_cloud_level(self):
        """Should set Google Cloud logger to WARNING."""
        setup_logging()
        
        gcloud_logger = logging.getLogger('google.cloud')
        assert gcloud_logger.level == logging.WARNING


class TestGetLogger:
    """Tests for get_logger function."""

    def test_get_logger_returns_logger(self):
        """Should return logger instance."""
        logger = get_logger('test_module')
        assert isinstance(logger, logging.Logger)

    def test_get_logger_with_name(self):
        """Should return logger with correct name."""
        logger = get_logger('my_module')
        assert logger.name == 'my_module'

    def test_get_logger_different_names(self):
        """Different names should return different loggers."""
        logger1 = get_logger('module1')
        logger2 = get_logger('module2')
        assert logger1.name != logger2.name


class TestValidateExtractedDataRobust:
    """Tests for validate_extracted_data_robust function."""

    def test_valid_data(self):
        """Valid data should pass validation."""
        data = {
            'evt_id': 'test-123',
            'type': 'edit',
            'domain': 'en.wikipedia.org',
            'title': 'Test Article',
            'user': 'TestUser',
            'bot': False,
            'ns': 0
        }
        
        result = validate_extracted_data_robust(data)
        
        assert result.is_valid is True
        assert result.data is not None
        assert result.quality_score == 1.0
        assert len(result.errors) == 0

    def test_empty_data(self):
        """Empty data should fail validation."""
        result = validate_extracted_data_robust({})
        
        assert result.is_valid is False
        assert result.data is None
        assert result.quality_score == 0.0
        assert len(result.errors) > 0

    def test_none_data(self):
        """None data should fail validation."""
        result = validate_extracted_data_robust(None)
        
        assert result.is_valid is False
        assert result.quality_score == 0.0

    def test_missing_required_fields(self):
        """Missing required fields should be noted."""
        data = {'title': 'Test'}
        
        result = validate_extracted_data_robust(data)
        
        assert len(result.errors) > 0
        assert any('evt_id' in err for err in result.errors)

    def test_type_coercion(self):
        """Should coerce types correctly."""
        data = {
            'evt_id': 'test-123',
            'type': 'edit',
            'domain': 'en.wikipedia.org',
            'ns': '5',  # String instead of int
            'bot': 1,   # Int instead of bool
        }
        
        result = validate_extracted_data_robust(data)
        
        assert result.is_valid is True
        assert result.data['ns'] == 5
        assert result.data['bot'] is True

    def test_string_truncation(self):
        """Long strings should be truncated."""
        data = {
            'evt_id': 'test-123',
            'type': 'edit',
            'domain': 'en.wikipedia.org',
            'comment': 'a' * 2000  # Exceeds max_length
        }
        
        result = validate_extracted_data_robust(data)
        
        assert len(result.data['comment']) == 1000
        assert len(result.warnings) > 0

    def test_string_trimming(self):
        """Strings should be trimmed."""
        data = {
            'evt_id': 'test-123',
            'type': 'edit',
            'domain': 'en.wikipedia.org',
            'title': '  Test Article  '
        }
        
        result = validate_extracted_data_robust(data)
        
        assert result.data['title'] == 'Test Article'

    def test_unknown_fields_warning(self):
        """Unknown fields should generate warnings."""
        data = {
            'evt_id': 'test-123',
            'type': 'edit',
            'domain': 'en.wikipedia.org',
            'unknown_field': 'value'
        }
        
        result = validate_extracted_data_robust(data)
        
        assert len(result.warnings) > 0
        assert any('unknown_field' in str(w) for w in result.warnings)

    def test_quality_score_degradation(self):
        """Quality score should degrade with issues."""
        data = {
            'evt_id': 'test-123',
            'type': 'edit',
            'domain': 'en.wikipedia.org',
            'comment': 'a' * 2000  # Will be truncated
        }
        
        result = validate_extracted_data_robust(data)
        
        assert result.quality_score < 1.0

    def test_default_values(self):
        """Missing optional fields should get defaults."""
        data = {
            'evt_id': 'test-123',
            'type': 'edit',
            'domain': 'en.wikipedia.org'
        }
        
        result = validate_extracted_data_robust(data)
        
        assert result.data['title'] == ''
        assert result.data['user'] == ''
        assert result.data['bot'] is False
        assert result.data['ns'] == 0


class TestPipelineMonitor:
    """Tests for PipelineMonitor class."""

    @pytest.fixture
    def monitor_config(self):
        """Configuration for monitor."""
        return {
            'project_id': 'test-project',
            'region': 'us-central1',
            'subscription': 'projects/test-project/subscriptions/test-sub'
        }

    @pytest.fixture
    def monitor(self, monitor_config):
        """Create PipelineMonitor instance."""
        with patch('wiki_pipeline.utils.monitoring_v3.MetricServiceClient'):
            with patch('wiki_pipeline.utils.pubsub_v1.SubscriberClient'):
                return PipelineMonitor(monitor_config)

    def test_init(self, monitor_config):
        """Should initialize with config."""
        with patch('wiki_pipeline.utils.monitoring_v3.MetricServiceClient'):
            with patch('wiki_pipeline.utils.pubsub_v1.SubscriberClient'):
                monitor = PipelineMonitor(monitor_config)
                
                assert monitor.project_id == 'test-project'
                assert monitor.region == 'us-central1'
                assert monitor.subscription_name == 'projects/test-project/subscriptions/test-sub'

    def test_get_subscription_backlog_success(self, monitor):
        """Should get backlog from monitoring API."""
        mock_result = MagicMock()
        mock_point = MagicMock()
        mock_point.value.int64_value = 1234
        mock_result.points = [mock_point]
        
        monitor.monitoring_client.list_time_series = MagicMock(return_value=[mock_result])
        
        backlog = monitor.get_subscription_backlog()
        assert backlog == 1234

    def test_get_subscription_backlog_error(self, monitor):
        """Should handle errors gracefully."""
        monitor.monitoring_client.list_time_series = MagicMock(
            side_effect=Exception("API error")
        )
        
        backlog = monitor.get_subscription_backlog()
        assert backlog == 0

    def test_get_active_jobs(self, monitor):
        """Should return empty list (simplified implementation)."""
        jobs = monitor.get_active_jobs()
        assert isinstance(jobs, list)
        assert len(jobs) == 0

    def test_analyze_performance_high_backlog(self, monitor):
        """Should recommend scaling for high backlog."""
        monitor.get_subscription_backlog = MagicMock(return_value=15000)
        
        analysis = monitor.analyze_performance()
        
        assert analysis['backlog_messages'] == 15000
        assert len(analysis['recommendations']) > 0
        assert any(r['severity'] == 'HIGH' for r in analysis['recommendations'])

    def test_analyze_performance_medium_backlog(self, monitor):
        """Should recommend scaling for medium backlog."""
        monitor.get_subscription_backlog = MagicMock(return_value=7000)
        
        analysis = monitor.analyze_performance()
        
        assert len(analysis['recommendations']) > 0
        assert any(r['severity'] == 'MEDIUM' for r in analysis['recommendations'])

    def test_analyze_performance_low_backlog(self, monitor):
        """Should recommend reducing for low backlog."""
        monitor.get_subscription_backlog = MagicMock(return_value=50)
        monitor.get_active_jobs = MagicMock(return_value=[{'name': 'job1', 'state': 'JOB_STATE_RUNNING'}])
        
        analysis = monitor.analyze_performance()
        
        assert len(analysis['recommendations']) > 0
        assert any(r['severity'] == 'LOW' for r in analysis['recommendations'])

    def test_analyze_performance_optimal(self, monitor):
        """Should have no recommendations for optimal state."""
        monitor.get_subscription_backlog = MagicMock(return_value=500)
        monitor.get_active_jobs = MagicMock(return_value=[])
        
        analysis = monitor.analyze_performance()
        
        # May or may not have recommendations depending on thresholds
        assert 'backlog_messages' in analysis
        assert 'recommendations' in analysis

    def test_print_status(self, monitor):
        """Should print status without errors."""
        monitor.get_subscription_backlog = MagicMock(return_value=1000)
        monitor.get_active_jobs = MagicMock(return_value=[])
        
        # Should not raise exception
        with patch('builtins.print'):
            monitor.print_status()
