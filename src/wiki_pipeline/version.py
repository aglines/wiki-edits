"""
Single source of truth for detection rule version and changelog.

This module centralizes version management to prevent version mismatches
across the codebase. All version references should import from here.
"""

from typing import Dict
from datetime import datetime

DETECTION_RULE_VERSION = "1.0.3"

RULE_CHANGELOG: Dict[str, Dict[str, str]] = {
    "1.0.3": {
        "date": "2024-12-12",
        "changes": "Added full list of emojis as maintained by unicode.org, expanding from a small set in 1.0.2. To update this list periodically, run scripts/update_emojis.py"
    },
    "1.0.2": {
        "date": "2024-11-XX",
        "changes": "Added emoji detection, chatgpt attribution objects, phrasal templates, markdown syntax. Externalized all patterns to JSON configuration"
    },
    "1.0.1": {
        "date": "2024-11-XX",
        "changes": "Added knowledge cutoff disclaimers and collaborative communication patterns"
    },
    "1.0.0": {
        "date": "2024-11-XX",
        "changes": "Initial rule version. Includes: chatgpt artifacts, chatgpt urls, prompt refusal, markdown syntax, excessive em dashes, promotional language, importance emphasis"
    }
}


def get_version() -> str:
    """Get current detection rule version."""
    return DETECTION_RULE_VERSION


def get_changelog(version: str = None) -> str:
    """
    Get changelog for a specific version or all versions.
    
    Args:
        version: Specific version to get changelog for. If None, returns all.
        
    Returns:
        Formatted changelog string
    """
    if version:
        if version not in RULE_CHANGELOG:
            return f"No changelog found for version {version}"
        entry = RULE_CHANGELOG[version]
        return f"v{version} ({entry['date']}): {entry['changes']}"
    
    # Return all changelogs
    lines = ["Detection Rule Changelog", "=" * 50]
    for ver in sorted(RULE_CHANGELOG.keys(), reverse=True):
        entry = RULE_CHANGELOG[ver]
        lines.append(f"\nv{ver} ({entry['date']}):")
        lines.append(f"  {entry['changes']}")
    return "\n".join(lines)


def validate_version_format(version: str) -> bool:
    """
    Validate version follows semantic versioning format (X.Y.Z).
    
    Args:
        version: Version string to validate
        
    Returns:
        True if valid, False otherwise
    """
    import re
    return bool(re.match(r'^\d+\.\d+\.\d+$', version))
