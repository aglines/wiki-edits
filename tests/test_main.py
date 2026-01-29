"""Unit tests for main entry point."""
import pytest
import sys
from unittest.mock import patch, MagicMock
from io import StringIO

from main import main, run_stream_mode


class TestMain:
    """Tests for main function."""

    @patch('main.run_local_pipeline')
    @patch('main.validate_config')
    @patch('main.setup_logging')
    def test_local_mode(self, mock_logging, mock_config, mock_local):
        """Should run local mode."""
        mock_config.return_value = {}
        
        with patch.object(sys, 'argv', ['main.py', '--mode', 'local']):
            main()
        
        mock_local.assert_called_once()

    @patch('main.run_local_pipeline')
    @patch('main.validate_config')
    @patch('main.setup_logging')
    def test_local_mode_with_max_events(self, mock_logging, mock_config, mock_local):
        """Should pass max_events to local mode."""
        mock_config.return_value = {}
        
        with patch.object(sys, 'argv', ['main.py', '--mode', 'local', '--max-events', '50']):
            main()
        
        mock_local.assert_called_once_with(50)

    @patch('main.run_dataflow_pipeline')
    @patch('main.validate_config')
    @patch('main.setup_logging')
    def test_pipeline_mode(self, mock_logging, mock_config, mock_pipeline):
        """Should run pipeline mode."""
        mock_config.return_value = {
            'topic': 'test-topic',
            'subscription': 'test-sub',
            'output_table': 'test-table'
        }
        
        with patch.object(sys, 'argv', ['main.py', '--mode', 'pipeline']):
            main()
        
        mock_pipeline.assert_called_once()

    @patch('main.validate_config')
    @patch('main.setup_logging')
    def test_pipeline_mode_missing_config(self, mock_logging, mock_config):
        """Should exit if required config missing."""
        mock_config.return_value = {}
        
        with patch.object(sys, 'argv', ['main.py', '--mode', 'pipeline']):
            with pytest.raises(SystemExit):
                main()

    @patch('main.asyncio.run')
    @patch('main.validate_config')
    @patch('main.setup_logging')
    def test_stream_mode(self, mock_logging, mock_config, mock_async):
        """Should run stream mode."""
        mock_config.return_value = {'topic': 'test-topic'}
        
        with patch.object(sys, 'argv', ['main.py', '--mode', 'stream']):
            main()
        
        mock_async.assert_called_once()

    @patch('main.validate_config')
    @patch('main.setup_logging')
    def test_cli_overrides_config(self, mock_logging, mock_config):
        """CLI arguments should override config."""
        mock_config.return_value = {
            'topic': 'config-topic',
            'subscription': 'config-sub',
            'output_table': 'config-table'
        }
        
        with patch('main.run_dataflow_pipeline') as mock_pipeline:
            with patch.object(sys, 'argv', [
                'main.py',
                '--mode', 'pipeline',
                '--topic', 'cli-topic',
                '--subscription', 'cli-sub'
            ]):
                main()
            
            call_args = mock_pipeline.call_args[0][0]
            assert call_args['topic'] == 'cli-topic'
            assert call_args['subscription'] == 'cli-sub'

    @patch('main.validate_config')
    @patch('main.setup_logging')
    def test_config_error_handling(self, mock_logging, mock_config):
        """Should handle ConfigError gracefully."""
        from wiki_pipeline.utils import ConfigError
        mock_config.side_effect = ConfigError("Missing config")
        
        with patch.object(sys, 'argv', ['main.py', '--mode', 'local']):
            with patch('main.run_local_pipeline'):
                main()

    @patch('main.validate_config')
    @patch('main.setup_logging')
    def test_general_exception_handling(self, mock_logging, mock_config):
        """Should handle general exceptions."""
        mock_config.return_value = {}
        
        with patch('main.run_local_pipeline', side_effect=Exception("Test error")):
            with patch.object(sys, 'argv', ['main.py', '--mode', 'local']):
                with pytest.raises(SystemExit):
                    main()

    @patch('main.validate_config')
    @patch('main.setup_logging')
    def test_default_mode_is_pipeline(self, mock_logging, mock_config):
        """Default mode should be pipeline."""
        mock_config.return_value = {
            'topic': 'test-topic',
            'subscription': 'test-sub',
            'output_table': 'test-table'
        }
        
        with patch('main.run_dataflow_pipeline') as mock_pipeline:
            with patch.object(sys, 'argv', ['main.py']):
                main()
            
            mock_pipeline.assert_called_once()


class TestRunStreamMode:
    """Tests for run_stream_mode function."""

    @pytest.mark.asyncio
    @patch('main.WikimediaStreamer')
    @patch.dict('os.environ', {'GCP_PROJECT_ID': 'test-project'})
    async def test_stream_mode_basic(self, mock_streamer_class):
        """Should create streamer and call stream_to_pubsub."""
        mock_streamer = MagicMock()
        mock_streamer_class.return_value = mock_streamer
        
        config = {'topic': 'test-topic'}
        await run_stream_mode(config)
        
        mock_streamer_class.assert_called_once_with('test-project', 'test-topic')
        mock_streamer.stream_to_pubsub.assert_called_once()

    @pytest.mark.asyncio
    @patch('main.WikimediaStreamer')
    @patch.dict('os.environ', {'GCP_PROJECT_ID': 'test-project'})
    async def test_stream_mode_with_max_events(self, mock_streamer_class):
        """Should pass max_events to streamer."""
        mock_streamer = MagicMock()
        mock_streamer_class.return_value = mock_streamer
        
        config = {'topic': 'test-topic'}
        await run_stream_mode(config, max_events=100)
        
        mock_streamer.stream_to_pubsub.assert_called_once_with(100)

    @pytest.mark.asyncio
    @patch('main.WikimediaStreamer')
    @patch('main.TimeoutContext')
    @patch.dict('os.environ', {'GCP_PROJECT_ID': 'test-project'})
    async def test_stream_mode_with_timeout(self, mock_timeout_class, mock_streamer_class):
        """Should use TimeoutContext when timeout specified."""
        mock_streamer = MagicMock()
        mock_streamer_class.return_value = mock_streamer
        
        mock_timeout = MagicMock()
        mock_timeout.__enter__ = MagicMock(return_value=mock_timeout)
        mock_timeout.__exit__ = MagicMock(return_value=False)
        mock_timeout_class.return_value = mock_timeout
        
        config = {'topic': 'test-topic'}
        await run_stream_mode(config, timeout_hours=2)
        
        mock_timeout_class.assert_called_once_with(hours=2)

    @pytest.mark.asyncio
    @patch('main.TimeoutContext')
    @patch('main.WikimediaStreamer')
    @patch.dict('os.environ', {'GCP_PROJECT_ID': 'test-project'})
    async def test_stream_mode_handles_keyboard_interrupt(self, mock_streamer_class, mock_timeout_class):
        """Should handle KeyboardInterrupt gracefully."""
        mock_streamer = MagicMock()
        mock_streamer.stream_to_pubsub.side_effect = KeyboardInterrupt()
        mock_streamer_class.return_value = mock_streamer
        
        mock_timeout = MagicMock()
        mock_timeout.__enter__ = MagicMock(return_value=mock_timeout)
        mock_timeout.__exit__ = MagicMock(return_value=False)
        mock_timeout_class.return_value = mock_timeout
        
        config = {'topic': 'test-topic'}
        # Should not raise exception
        await run_stream_mode(config)
