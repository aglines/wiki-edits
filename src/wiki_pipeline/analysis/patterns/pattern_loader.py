"""Pattern loader for AI detection rules."""

import json
import os
from functools import lru_cache
from typing import Dict, Any

@lru_cache(maxsize=1)
def load_detection_patterns() -> Dict[str, Any]:
    """Load detection patterns from JSON file with caching."""
    patterns_file = os.path.join(os.path.dirname(__file__), 'detection_patterns.json')
    
    try:
        with open(patterns_file, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        # Fallback to empty patterns if file loading fails
        print(f"Warning: Could not load detection patterns: {e}")
        return {"patterns": {}}

def get_patterns() -> Dict[str, Any]:
    """Get detection patterns."""
    return load_detection_patterns()["patterns"]
