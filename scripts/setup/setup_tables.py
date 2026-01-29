#!/usr/bin/env python3
"""Simple script to create BigQuery tables for staging environment."""
from wiki_pipeline.schema_migration import ensure_pipeline_tables
from wiki_pipeline.utils import validate_config, ConfigError, setup_logging, get_logger

def main():
    """Create BigQuery tables if they don't exist."""
    setup_logging(level="INFO", structured=False)
    logger = get_logger(__name__)
    
    try:
        # Load config
        config = validate_config()
        logger.info(f"Setting up tables for project: {config['project_id']}")
        
        # Parse table reference
        table_parts = config['output_table'].split('.')
        if len(table_parts) != 3:
            raise ValueError(f"Invalid table format: {config['output_table']}. Expected: project.dataset.table")
        
        project_id, dataset_id, table_name = table_parts
        
        # Create tables
        logger.info("🔧 Creating BigQuery tables...")
        success = ensure_pipeline_tables(project_id, dataset_id, table_name)
        
        if success:
            logger.info("✅ Tables are ready!")
            logger.info(f"Main table: {config['output_table']}")
            logger.info(f"Dead letter: {config['output_table']}_dead_letter")
        else:
            logger.error("❌ Failed to create tables")
            sys.exit(1)
            
    except ConfigError as e:
        logger.error(f"Configuration error: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Setup failed: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()