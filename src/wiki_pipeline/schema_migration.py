"""Automated schema migration for BigQuery tables."""
import logging
from typing import Dict, List, Any, Optional
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from dotenv import load_dotenv
from .schema import BQ_SCHEMA, DLQ_SCHEMA, SCHEMA_VERSION

# Load environment variables from .env file
load_dotenv()

logger = logging.getLogger(__name__)


class SchemaMigrationManager:
    """Handles automated BigQuery schema migrations and compatibility checks."""
    
    def __init__(self, project_id: str):
        self.client = bigquery.Client(project=project_id)
        self.project_id = project_id
    
    def ensure_table_schema(self, table_id: str, schema_string: str, 
                           is_dlq_table: bool = False) -> bool:
        """
        Ensure BigQuery table exists with correct schema, creating or updating as needed.
        
        Args:
            table_id: Full table ID (project.dataset.table)
            schema_string: BigQuery schema string format
            is_dlq_table: Whether this is a dead letter queue table
            
        Returns:
            True if table is ready, False if migration failed
        """
        try:
            # Parse table reference
            table_ref = bigquery.TableReference.from_string(table_id)
            
            # Check if table exists
            try:
                table = self.client.get_table(table_ref)
                logger.info(f"Table {table_id} exists, checking schema compatibility")
                
                # Compare schemas and migrate if needed
                return self._migrate_schema_if_needed(table, schema_string, is_dlq_table)
                
            except NotFound:
                logger.info(f"Table {table_id} not found, creating new table")
                return self._create_table_with_schema(table_ref, schema_string)
                
        except Exception as e:
            logger.error(f"Failed to ensure table schema for {table_id}: {e}")
            return False
    
    def _create_table_with_schema(self, table_ref: bigquery.TableReference, 
                                 schema_string: str) -> bool:
        """Create new table with given schema."""
        try:
            # Parse schema string into BigQuery schema
            schema = self._parse_schema_string(schema_string)
            
            # Create table
            table = bigquery.Table(table_ref, schema=schema)
            
            # Set table options
            table.description = f"Wikipedia edits pipeline table (schema v{SCHEMA_VERSION})"
            table.labels = {"pipeline": "wiki-edits", "schema_version": SCHEMA_VERSION.replace(".", "_")}
            
            # Enable partitioning on timestamp fields for better performance
            if any(field.name in ['dt_server', '_validation_timestamp'] for field in schema):
                partition_field = 'dt_server' if any(f.name == 'dt_server' for f in schema) else '_validation_timestamp'
                table.time_partitioning = bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field=partition_field
                )
            
            created_table = self.client.create_table(table)
            logger.info(f"✅ Created table {created_table.table_id} with {len(schema)} fields")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create table: {e}")
            return False
    
    def _migrate_schema_if_needed(self, table: bigquery.Table, 
                                 target_schema_string: str, is_dlq_table: bool) -> bool:
        """Check if schema migration is needed and perform it."""
        try:
            target_schema = self._parse_schema_string(target_schema_string)
            current_schema = table.schema
            
            # Compare schemas
            migration_needed, new_fields = self._compare_schemas(current_schema, target_schema)
            
            if not migration_needed:
                logger.info(f"✅ Schema for {table.table_id} is up to date")
                return True
            
            logger.info(f"🔄 Schema migration needed for {table.table_id}: adding {len(new_fields)} fields")
            
            # Perform additive schema migration (BigQuery only supports adding fields)
            return self._perform_additive_migration(table, new_fields)
            
        except Exception as e:
            logger.error(f"Schema migration failed: {e}")
            return False
    
    def _compare_schemas(self, current: List[bigquery.SchemaField], 
                        target: List[bigquery.SchemaField]) -> tuple[bool, List[bigquery.SchemaField]]:
        """Compare current and target schemas to determine migration needs."""
        current_field_names = {field.name for field in current}
        target_field_names = {field.name for field in target}
        
        # Find new fields to add
        new_field_names = target_field_names - current_field_names
        new_fields = [field for field in target if field.name in new_field_names]
        
        # Check for removed fields (not supported in BigQuery additive migrations)
        removed_fields = current_field_names - target_field_names
        if removed_fields:
            logger.warning(f"Cannot remove fields in BigQuery: {removed_fields}. "
                         f"These will remain in the table schema.")
        
        migration_needed = len(new_fields) > 0
        return migration_needed, new_fields
    
    def _perform_additive_migration(self, table: bigquery.Table, 
                                   new_fields: List[bigquery.SchemaField]) -> bool:
        """Perform additive schema migration by adding new fields."""
        try:
            # Create updated schema
            updated_schema = list(table.schema) + new_fields
            
            # Update table schema
            table.schema = updated_schema
            updated_table = self.client.update_table(table, ["schema"])
            
            logger.info(f"✅ Successfully added {len(new_fields)} fields to {table.table_id}")
            
            # Log the new fields for monitoring
            field_names = [field.name for field in new_fields]
            logger.info(f"Added fields: {field_names}")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to perform additive migration: {e}")
            return False
    
    def _parse_schema_string(self, schema_string: str) -> List[bigquery.SchemaField]:
        """Parse BigQuery schema string into SchemaField objects."""
        fields = []
        
        for line in schema_string.strip().split('\n'):
            line = line.strip().rstrip(',')
            if not line:
                continue
                
            try:
                # Parse field definition: name:TYPE
                name, type_def = line.split(':', 1)
                name = name.strip()
                type_def = type_def.strip()
                
                # Handle nullable fields (default is nullable unless specified)
                mode = "NULLABLE"
                if type_def.endswith('!'):
                    mode = "REQUIRED"
                    type_def = type_def[:-1]
                
                # Map BigQuery types
                field_type = type_def.upper()
                
                fields.append(bigquery.SchemaField(name, field_type, mode=mode))
                
            except ValueError as e:
                logger.error(f"Failed to parse schema line '{line}': {e}")
                continue
        
        return fields
    
    def validate_schema_compatibility(self, old_schema_string: str, 
                                    new_schema_string: str) -> Dict[str, Any]:
        """Validate that schema changes are backward compatible."""
        try:
            old_schema = self._parse_schema_string(old_schema_string)
            new_schema = self._parse_schema_string(new_schema_string)
            
            old_fields = {field.name: field for field in old_schema}
            new_fields = {field.name: field for field in new_schema}
            
            compatibility_report = {
                "is_compatible": True,
                "added_fields": [],
                "removed_fields": [],
                "type_changes": [],
                "mode_changes": []
            }
            
            # Check for added fields (always compatible)
            added = set(new_fields.keys()) - set(old_fields.keys())
            compatibility_report["added_fields"] = list(added)
            
            # Check for removed fields (breaking change)
            removed = set(old_fields.keys()) - set(new_fields.keys())
            compatibility_report["removed_fields"] = list(removed)
            if removed:
                compatibility_report["is_compatible"] = False
            
            # Check for type/mode changes in existing fields
            for field_name in set(old_fields.keys()) & set(new_fields.keys()):
                old_field = old_fields[field_name]
                new_field = new_fields[field_name]
                
                if old_field.field_type != new_field.field_type:
                    compatibility_report["type_changes"].append({  # type: ignore
                        "field": field_name,
                        "old_type": old_field.field_type,
                        "new_type": new_field.field_type
                    })
                    compatibility_report["is_compatible"] = False

                if old_field.mode != new_field.mode:
                    compatibility_report["mode_changes"].append({  # type: ignore
                        "field": field_name,
                        "old_mode": old_field.mode,
                        "new_mode": new_field.mode
                    })
                    # Making required fields nullable is compatible, opposite is not
                    if old_field.mode == "NULLABLE" and new_field.mode == "REQUIRED":
                        compatibility_report["is_compatible"] = False
            
            return compatibility_report
            
        except Exception as e:
            logger.error(f"Schema compatibility validation failed: {e}")
            return {"is_compatible": False, "error": str(e)}


def ensure_pipeline_tables(project_id: str, dataset_id: str, table_name: str) -> bool:
    """
    Ensure all pipeline tables exist with correct schemas.
    
    Args:
        project_id: Google Cloud project ID
        dataset_id: BigQuery dataset ID
        table_name: Base table name
        
    Returns:
        True if all tables are ready, False otherwise
    """
    migration_manager = SchemaMigrationManager(project_id)
    
    main_table_id = f"{project_id}.{dataset_id}.{table_name}"
    dlq_table_id = f"{project_id}.{dataset_id}.{table_name}_dead_letter"
    
    logger.info("🔧 Ensuring pipeline tables exist with correct schemas")
    
    # Ensure main table
    main_table_ready = migration_manager.ensure_table_schema(main_table_id, BQ_SCHEMA, False)
    
    # Ensure dead letter queue table
    dlq_table_ready = migration_manager.ensure_table_schema(dlq_table_id, DLQ_SCHEMA, True)
    
    if main_table_ready and dlq_table_ready:
        logger.info("✅ All pipeline tables are ready")
        return True
    else:
        logger.error("❌ Failed to ensure pipeline tables are ready")
        return False