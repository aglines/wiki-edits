"""AI detection logic for Wikipedia edits."""

import re
import json
import os
from functools import lru_cache
from typing import Dict, Any

# Import version from centralized source
from wiki_pipeline.version import DETECTION_RULE_VERSION, get_changelog

# For changelog history, see: src/wiki_pipeline/version.py
# Or run: python -c "from wiki_pipeline.version import get_changelog; print(get_changelog())" 


# ============================================================================
# PATTERN LOADER
# ============================================================================

@lru_cache(maxsize=1)
def load_detection_patterns() -> Dict[str, Any]:
    """Load detection patterns from JSON file with caching."""
    patterns_file = os.path.join(os.path.dirname(__file__), 'detection_patterns.json')
    
    try:
        with open(patterns_file, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        # Graceful fallback: AI detection disabled but pipeline continues
        import logging
        logging.warning(f"AI detection patterns not available: {e}. AI detection will be disabled.")
        return {"patterns": {}}

@lru_cache(maxsize=1)
def load_emoji_list() -> set:
    """Load external emoji list from assets with caching."""
    emoji_file = os.path.join(os.path.dirname(__file__), 'assets', 'emoji.json')
    
    try:
        with open(emoji_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
            return set(data.get('emojis', []))
    except (FileNotFoundError, json.JSONDecodeError) as e:
        # Graceful fallback: return empty set
        import logging
        logging.warning(f"Emoji list not available: {e}. Emoji detection will be disabled.")
        return set()

def get_patterns() -> Dict[str, Any]:
    """Get detection patterns."""
    return load_detection_patterns()["patterns"]


# ============================================================================
# AI DETECTION
# ============================================================================

def detect_ai_indicators(content: Dict[str, Any]) -> Dict[str, Any]:
    """Detect AI writing indicators in content."""
    flags = {}
    patterns = get_patterns()

    # Get text fields
    text_fields = []
    for field_name in ['comment', 'parsed_comment', 'title']:
        field_value = content.get(field_name, '')
        if field_value and isinstance(field_value, str):
            text_fields.append(field_value)

    full_text = ' '.join(text_fields)

    # Convert to lowercase once for consistent case-insensitive matching
    full_text_lower = full_text.lower()

    # Level 1: Single character or string matching
    if any(artifact in full_text_lower for artifact in patterns.get('chatgpt_artifacts', [])):
        flags['chatgpt_artifacts'] = True

    if any(attr in full_text_lower for attr in patterns.get('chatgpt_attribution', [])):
        flags['chatgpt_attribution'] = True

    if any(phrase in full_text_lower for phrase in patterns.get('prompt_refusal', [])):
        flags['prompt_refusal'] = True

    # Emoji detection in headings/lists
    emoji_config = patterns.get('emojis', {})
    if emoji_config.get('use_external_list', False):
        # Use external emoji list
        emoji_set = load_emoji_list()
        if emoji_set and any(char in emoji_set for char in full_text):
            flags['emoji_in_headings'] = True
    else:
        # Fallback to pattern-based detection
        emoji_pattern = emoji_config.get('pattern', '')
        if emoji_pattern and re.search(emoji_pattern, full_text):
            flags['emoji_in_headings'] = True

    # Phrasal templates detection
    placeholder_pattern = patterns.get('placeholder_templates', {}).get('pattern', '')
    if placeholder_pattern and re.search(placeholder_pattern, full_text, re.IGNORECASE):
        flags['placeholder_templates'] = True

    if any(phrase in full_text_lower for phrase in patterns.get('knowledge_cutoff_disclaimers', [])):
        flags['knowledge_cutoff_disclaimers'] = True

    if any(phrase in full_text_lower for phrase in patterns.get('collaborative_communication', [])):
        flags['collaborative_communication'] = True

    # Level 2: Simple patterns
    markdown_config = patterns.get('markdown_syntax', {})
    markdown_pattern = markdown_config.get('pattern', '')
    if markdown_pattern and re.search(markdown_pattern, full_text, re.MULTILINE):
        # Check exclude pattern if specified
        exclude_pattern = markdown_config.get('exclude_pattern', '')
        if not exclude_pattern or not re.search(exclude_pattern, full_text):
            flags['markdown_syntax'] = True

    # Dynamic threshold based on content length
    em_dash_count = full_text.count('—')
    content_length = len(full_text)
    if content_length > 0:
        em_dash_ratio = em_dash_count / content_length * 1000  # Per 1000 chars
        if em_dash_ratio > 2:  # More than 2 em-dashes per 1000 characters
            flags['excessive_em_dashes'] = True

    # Level 3: Phrase detection (already case-insensitive)
    if any(phrase in full_text_lower for phrase in patterns.get('promotional_language', [])):
        flags['promotional_language'] = True

    if any(phrase in full_text_lower for phrase in patterns.get('importance_emphasis', [])):
        flags['importance_emphasis'] = True

    return flags
