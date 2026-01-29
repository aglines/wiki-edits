"""AI detection logic for Wikipedia edits."""

from typing import Dict, Any

# Rule version for tracking detection evolution
DETECTION_RULE_VERSION = "1.0.1"
# initial rule version. includes:
# chatgpt artifacts, chatgpt urls, prompt refusal, markdown syntax, excessive em dashes, 
#   promotional language, importance emphasis

# added from 1.0.0: 
#   knowledge cutoff disclaimers
#   collaborative communication


def detect_ai_indicators(content: Dict[str, Any]) -> Dict[str, Any]:
    """Detect AI writing indicators in content."""
    flags = {}

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
    if 'turn0search' in full_text_lower or 'oaicite:' in full_text_lower:
        flags['chatgpt_artifacts'] = True

    if 'utm_source=chatgpt.com' in full_text_lower:
        flags['chatgpt_urls'] = True

    if any(phrase in full_text_lower for phrase in ['as an ai language model', 
        'as a large language model', 'OpenAI use policy']):
        flags['prompt_refusal'] = True

    if any(phrase in full_text_lower for phrase in ['up to my last training update', 'based on available information',
        'while specific details are limited','not widely available' ,'not widely documented']):
        flags['knowledge_cutoff_disclaimers'] = True

    if any(phrase in full_text_lower for phrase in ['i hope this helps', 'certainly!', 
        'let me know', 'would you like']):
        flags['collaborative_communication'] = True

    # Level 2: Simple patterns
    if '**' in full_text and "'''" not in full_text:
        flags['markdown_syntax'] = True

    # Dynamic threshold based on content length
    em_dash_count = full_text.count('—')
    content_length = len(full_text)
    if content_length > 0:
        em_dash_ratio = em_dash_count / content_length * 1000  # Per 1000 chars
        if em_dash_ratio > 2:  # More than 2 em-dashes per 1000 characters
            flags['excessive_em_dashes'] = True

    # Level 3: Phrase detection (already case-insensitive)
    promo_phrases = ['rich cultural heritage', 'breathtaking', 'nestled', 'stunning natural beauty']
    if any(phrase in full_text_lower for phrase in promo_phrases):
        flags['promotional_language'] = True

    importance_phrases = ['stands as a testament', 'serves as a testament', 'plays a vital role', 'plays a significant role',
         'continues to captivate', 'watershed moment', 'deeply rooted', 'profound heritage']
    if any(phrase in full_text_lower for phrase in importance_phrases):
        flags['importance_emphasis'] = True

    return flags
