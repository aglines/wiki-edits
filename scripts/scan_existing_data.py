#!/usr/bin/env python3
"""Scan existing BigQuery data for AI indicators."""

import os
from typing import Dict, Any, List
from google.cloud import bigquery
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

from wiki_pipeline.ai_detection import detect_ai_indicators
from wiki_pipeline.version import DETECTION_RULE_VERSION
from wiki_pipeline.schema import AI_SCHEMA


def scan_bigquery_data(project_id: str, dataset_id: str, table_id: str, batch_size: int = 10000):
    """Scan existing BigQuery data and write AI events to side table."""
    from datetime import datetime
    
    client = bigquery.Client(project=project_id)
    ai_table_id = f"{table_id}_ai_events"
    clean_table_id = f"{table_id}_ai_clean"
    
    total_processed = 0
    total_ai_found = 0
    
    while True:
        # Query records not in either ai_events or ai_clean for this rule version
        query = f"""
        SELECT evt_id, comment, parsed_comment, title, user, domain, ns, type, dt_server
        FROM `{project_id}.{dataset_id}.{table_id}` main
        WHERE NOT EXISTS (
            SELECT 1 FROM `{project_id}.{dataset_id}.{ai_table_id}` ai
            WHERE ai.event_id = main.evt_id AND ai.rule_version = '{DETECTION_RULE_VERSION}'
        )
        AND NOT EXISTS (
            SELECT 1 FROM `{project_id}.{dataset_id}.{clean_table_id}` clean
            WHERE clean.event_id = main.evt_id AND clean.rule_version = '{DETECTION_RULE_VERSION}'
        )
        LIMIT {batch_size}
        """
    
        query_job = client.query(query)
        results = list(query_job.result())
        
        if not results:
            print(f"Scan complete. Processed {total_processed} records, found {total_ai_found} AI events")
            break
        
        print(f"Processing batch of {len(results)} records... (Total scanned: {total_processed})")
        
        ai_events = []
        clean_records = []
        
        for row in results:
            content = {
                'comment': row.comment or '',
                'parsed_comment': row.parsed_comment or '',
                'title': row.title or ''
            }
            
            ai_flags = detect_ai_indicators(content)
            
            if ai_flags:
                ai_event = {
                    'event_id': row.evt_id,
                    'rule_version': DETECTION_RULE_VERSION,
                    'detection_timestamp': datetime.utcnow().isoformat(),
                    'timestamp': row.dt_server.isoformat() if row.dt_server else '',
                    'user': row.user or '',
                    'title': row.title or '',
                    'domain': row.domain or '',
                    'namespace': row.ns or 0,
                    'edit_type': row.type or '',
                    'ai_flags': ai_flags,
                    'content_sample': content,
                    'detection_metadata': {
                        'total_flags': len(ai_flags),
                        'flag_types': list(ai_flags.keys()),
                        'detected_at': row.dt_server.isoformat() if row.dt_server else '',
                        'rule_version': DETECTION_RULE_VERSION
                    }
                }
                ai_events.append(ai_event)
            else:
                # Track clean records
                clean_records.append({
                    'event_id': row.evt_id,
                    'rule_version': DETECTION_RULE_VERSION
                })
    
        # Write AI events
        if ai_events:
            ai_ref = client.dataset(dataset_id).table(ai_table_id)
            
            # DEBUG: DELETE LATER - Query actual table schema
            # try:
            #     existing_table = client.get_table(ai_ref)
            #     print("DEBUG - Existing table schema:")
            #     for field in existing_table.schema:
            #         print(f"  {field.name}: {field.field_type}")
            # except Exception as e:
            #     print(f"DEBUG - Could not fetch existing table: {e}")
            
            schema_fields = []
            for field_def in AI_SCHEMA.strip().split(','):
                field_name, field_type = field_def.split(':')
                # DEBUG: DELETE LATER
                # if 'timestamp' in field_name.lower():
                #     print(f"DEBUG - Parsing field_def: {repr(field_def)}")
                #     print(f"DEBUG - Split result: name={repr(field_name)}, type={repr(field_type)}")
                #     print(f"DEBUG - After strip: name={repr(field_name.strip())}, type={repr(field_type.strip())}")
                schema_fields.append(bigquery.SchemaField(field_name.strip(), field_type.strip()))
            
            # DEBUG: DELETE LATER - Print first event and schema
            # print("DEBUG - First AI event:")
            # import json
            # print(json.dumps(ai_events[0], indent=2, default=str))
            # print("\nDEBUG - Parsed schema fields:")
            # for field in schema_fields:
            #     print(f"  {field.name}: {field.field_type}")
            # print(f"DEBUG - detection_timestamp field type: {type(ai_events[0]['detection_timestamp'])}")
            # print(f"DEBUG - detection_timestamp field value: {repr(ai_events[0]['detection_timestamp'])}")
            # print(f"DEBUG - timestamp field type: {type(ai_events[0]['timestamp'])}")
            # print(f"DEBUG - timestamp field value: {repr(ai_events[0]['timestamp'])}")
            # print("DEBUG END\n")
            
            # Convert dict fields to JSON strings for streaming insert
            import json
            for event in ai_events:
                # Convert all JSON-type fields (dict or list) to JSON strings
                for field in ['ai_flags', 'content_sample', 'detection_metadata']:
                    value = event.get(field)
                    if value is not None and not isinstance(value, str):
                        event[field] = json.dumps(value)
            
            # Try using insert_rows_json instead of load_table_from_json
            errors = client.insert_rows_json(ai_ref, ai_events)
            if errors:
                print(f"ERROR inserting rows: {errors}")
            else:
                print(f"  Found {len(ai_events)} AI events")
        
        # Write clean records
        if clean_records:
            clean_ref = client.dataset(dataset_id).table(clean_table_id)
            clean_schema = [
                bigquery.SchemaField('event_id', 'STRING'),
                bigquery.SchemaField('rule_version', 'STRING')
            ]
            clean_config = bigquery.LoadJobConfig(
                schema=clean_schema,
                write_disposition="WRITE_APPEND",
                create_disposition="CREATE_IF_NEEDED"
            )
            client.load_table_from_json(clean_records, clean_ref, clean_config).result()
        
        total_processed += len(results)
        total_ai_found += len(ai_events)
    
    return total_ai_found


if __name__ == "__main__":
    # Load from environment variables
    project_id = os.getenv('GCP_PROJECT')
    dataset_id = os.getenv('BQ_DATASET')
    table_id = os.getenv('BQ_TABLE')
    
    scan_bigquery_data(project_id, dataset_id, table_id)
