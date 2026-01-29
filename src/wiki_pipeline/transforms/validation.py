"""Data validation utilities for Wikipedia edit events."""
import logging
from datetime import datetime
from typing import Dict, Any

import apache_beam as beam
from ..schema import schema_validator, ValidationError

logger = logging.getLogger(__name__)


class ValidationTransform(beam.DoFn):
    """Enhanced data validation with structured error categorization."""
    
    def process(self, element):
        """Validate and route data to appropriate outputs with detailed error categorization."""
        try:
            if not element or not isinstance(element, dict):
                # Create basic validation error for malformed input
                errors = [ValidationError(
                    category='MALFORMED_INPUT',
                    field='_root',
                    expected_type='DICT',
                    actual_value=element,
                    message='Input is not a valid dictionary'
                )]
                dlq_record = schema_validator.create_dlq_record({}, errors)
                yield beam.pvalue.TaggedOutput('invalid', dlq_record)
                return
            
            # Perform comprehensive validation
            is_valid, validation_errors = schema_validator.validate_record(element)
            
            if is_valid:
                yield beam.pvalue.TaggedOutput('valid', element)
            else:
                # Create structured dead letter record with detailed error information
                dlq_record = schema_validator.create_dlq_record(element, validation_errors)
                yield beam.pvalue.TaggedOutput('invalid', dlq_record)
                
                # Log validation failures for monitoring
                error_categories = {error.category for error in validation_errors}
                logger.warning(f"Validation failed for record {element.get('evt_id', 'unknown')}: {error_categories}")
                
        except Exception as e:
            # Handle unexpected validation errors
            logger.error(f"Unexpected error during validation: {e}")
            
            fallback_errors = [ValidationError(
                category='VALIDATION_EXCEPTION',
                field='_system',
                expected_type='ANY',
                actual_value=str(e),
                message=f'Unexpected validation error: {str(e)}'
            )]
            
            dlq_record = schema_validator.create_dlq_record(element or {}, fallback_errors)
            yield beam.pvalue.TaggedOutput('invalid', dlq_record)
