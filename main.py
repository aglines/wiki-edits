"""Wikipedia edits pipeline - professional entry point."""
import argparse
import asyncio
import sys

from pipeline import WikimediaStreamer, run_dataflow_pipeline, run_local_pipeline
from wiki_pipeline.utils import validate_config, ConfigError, setup_logging, get_logger, TimeoutContext, TimeoutError

logger = get_logger(__name__)


async def run_stream_mode(config: dict, max_events: int = None, timeout_hours: int = None) -> None:
    """Run streaming ingestion mode."""
    streamer = WikimediaStreamer(config['project_id'], config['topic'])
    
    try:
        with TimeoutContext(hours=timeout_hours):
            streamer.stream_to_pubsub(max_events)
    except TimeoutError as e:
        logger.info(f"Stream stopped due to timeout: {e}")
    except KeyboardInterrupt:
        logger.info("Stream stopped by user")


def main():
    """Entry point with mode selection."""
    parser = argparse.ArgumentParser(description='Wikipedia Edits Pipeline')
    parser.add_argument('mode', nargs='?', default='pipeline', choices=['local', 'stream', 'pipeline'], 
                       help='Pipeline mode: local (test), stream (ingest), pipeline (process). Default: pipeline')
    parser.add_argument('--max-events', type=int, help='Max events for local/stream modes')
    parser.add_argument('--time', type=int, help='Time limit in hours (stream mode only). Default: no limit')
    parser.add_argument('--project', help='Google Cloud project ID')
    parser.add_argument('--region', help='Google Cloud region')
    
    args = parser.parse_args()
    setup_logging(level="INFO", structured=True)
    
    try:
        config = validate_config()
        
        # Override with CLI args if provided
        if args.project:
            config['project_id'] = args.project
        if args.region:
            config['region'] = args.region
        
        logger.info(f"🚀 Starting Wikipedia pipeline in {args.mode} mode")
        
        if args.mode == 'local':
            run_local_pipeline(args.max_events or 20)
        elif args.mode == 'stream':
            asyncio.run(run_stream_mode(config, args.max_events, args.time))
        elif args.mode == 'pipeline':
            run_dataflow_pipeline(config)
            
    except ConfigError as e:
        logger.error(f"Configuration error: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
