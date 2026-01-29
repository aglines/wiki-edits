#!/usr/bin/env python3
"""Simple stats reader for AI events table."""

import os
from google.cloud import bigquery
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


def get_ai_events_count(project_id: str = None, dataset_id: str = None, table_id: str = None):
    """Get total row count from the ai_events table.
    
    Args:
        project_id: GCP project ID (defaults to env var)
        dataset_id: BigQuery dataset ID (defaults to env var)
        table_id: Base table ID (defaults to env var)
        
    Returns:
        int: Total number of rows in the ai_events table
    """
    # Use provided values or fall back to environment variables
    project_id = project_id or os.getenv('GCP_PROJECT')
    dataset_id = dataset_id or os.getenv('BQ_DATASET')
    table_id = table_id or os.getenv('BQ_TABLE')
    
    # Construct ai_events table name
    ai_table_id = f"{table_id}_ai_events"
    
    # Initialize BigQuery client
    client = bigquery.Client(project=project_id)
    
    # Simple count query
    query = f"""
    SELECT COUNT(*) as total_count
    FROM `{project_id}.{dataset_id}.{ai_table_id}`
    """
    
    try:
        query_job = client.query(query)
        result = list(query_job.result())
        return result[0].total_count
    except Exception as e:
        print(f"Error querying ai_events table: {e}")
        return None


def get_ai_flags_stats(project_id: str = None, dataset_id: str = None, table_id: str = None):
    """Get statistics about ai_flags JSON column values.
    
    Returns:
        list: List of dictionaries with flag_type and count
    """
    # Use provided values or fall back to environment variables
    project_id = project_id or os.getenv('GCP_PROJECT')
    dataset_id = dataset_id or os.getenv('BQ_DATASET')
    table_id = table_id or os.getenv('BQ_TABLE')
    
    # Construct ai_events table name
    ai_table_id = f"{table_id}_ai_events"
    
    # Initialize BigQuery client
    client = bigquery.Client(project=project_id)
    
    # Query to extract and count JSON keys from ai_flags
    query = f"""
    WITH flag_keys AS (
        SELECT key as flag_type
        FROM `{project_id}.{dataset_id}.{ai_table_id}`,
        UNNEST(JSON_KEYS(ai_flags)) as key
        WHERE ai_flags IS NOT NULL
    )
    SELECT 
        flag_type,
        COUNT(*) as count
    FROM flag_keys
    GROUP BY flag_type
    ORDER BY count DESC
    """
    
    try:
        query_job = client.query(query)
        results = list(query_job.result())
        return [{'flag_type': row.flag_type, 'count': row.count} for row in results]
    except Exception as e:
        print(f"Error querying ai_flags: {e}")
        return None


def get_ai_events_stats(project_id: str = None, dataset_id: str = None, table_id: str = None):
    """Get basic statistics from the ai_events table.
    
    Returns:
        dict: Dictionary with basic stats about the ai_events table
    """
    # Use provided values or fall back to environment variables
    project_id = project_id or os.getenv('GCP_PROJECT')
    dataset_id = dataset_id or os.getenv('BQ_DATASET')
    table_id = table_id or os.getenv('BQ_TABLE')
    
    # Construct ai_events table name
    ai_table_id = f"{table_id}_ai_events"
    
    # Initialize BigQuery client
    client = bigquery.Client(project=project_id)
    
    # Basic stats query
    query = f"""
    SELECT 
        COUNT(*) as total_count,
        COUNT(DISTINCT rule_version) as rule_versions,
        COUNT(DISTINCT user) as unique_users,
        COUNT(DISTINCT domain) as unique_domains
    FROM `{project_id}.{dataset_id}.{ai_table_id}`
    """
    
    try:
        query_job = client.query(query)
        result = list(query_job.result())
        row = result[0]
        
        return {
            'total_ai_events': row.total_count,
            'rule_versions': row.rule_versions,
            'unique_users': row.unique_users,
            'unique_domains': row.unique_domains
        }
    except Exception as e:
        print(f"Error querying ai_events table: {e}")
        return None


if __name__ == "__main__":
    # Simple command line usage
    count = get_ai_events_count()
    if count is not None:
        print(f"Total AI events: {count}")
    
    stats = get_ai_events_stats()
    if stats:
        print("\nAI Events Statistics:")
        for key, value in stats.items():
            print(f"  {key}: {value}")
    
    flag_stats = get_ai_flags_stats()
    if flag_stats:
        print("\nAI Flag Types and Counts:")
        for flag_info in flag_stats:
            print(f"  {flag_info['flag_type']}: {flag_info['count']}")
