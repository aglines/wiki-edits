#!/usr/bin/env python3
"""Basic usage example for the Wikipedia edits pipeline.

This example demonstrates:
1. How to process a single Wikipedia event
2. How to extract fields
3. How to detect AI content
"""

from wiki_pipeline.transforms.extractors import get_all_fields
from wiki_pipeline.ai_detection import detect_ai_indicators


def process_single_event():
    """Process a single Wikipedia edit event."""

    # Sample Wikipedia edit event (from Wikimedia SSE stream)
    sample_event = {
        'meta': {
            'id': 'abc123',
            'dt': '2024-01-15T10:30:00Z',
            'domain': 'en.wikipedia.org'
        },
        'type': 'edit',
        'title': 'Machine Learning',
        'user': 'ExampleUser',
        'bot': False,
        'timestamp': 1705318200,
        'namespace': 0,
        'wiki': 'enwiki',
        'minor': False,
        'patrolled': True,
        'length': {'old': 15000, 'new': 15250},
        'revision': {'old': 123456, 'new': 123457},
        'comment': 'Updated information about neural networks',
        'parsedcomment': '<b>Updated</b> information about neural networks'
    }

    print("Processing Wikipedia edit event:")
    print(f"  Title: {sample_event['title']}")
    print(f"  User: {sample_event['user']}")
    print(f"  Comment: {sample_event['comment']}")
    print()

    # Extract and normalize fields
    extracted = get_all_fields(sample_event)
    if extracted:
        print("Extracted fields:")
        print(f"  Event ID: {extracted['evt_id']}")
        print(f"  Type: {extracted['type']}")
        print(f"  Domain: {extracted['domain']}")
        print(f"  Namespace: {extracted['ns']}")
        print(f"  Length change: {extracted['len_new'] - extracted['len_old']} bytes")
        print()

    # Check for AI-generated content
    ai_flags = detect_ai_indicators(sample_event)
    if ai_flags:
        print("AI content detected:")
        for flag_type, detected in ai_flags.items():
            if detected:
                print(f"  - {flag_type}")
    else:
        print("No AI content indicators detected")


def process_ai_generated_event():
    """Process an event with AI-generated content."""

    # Event with potential AI indicators
    ai_event = {
        'meta': {
            'id': 'def456',
            'dt': '2024-01-15T11:00:00Z',
            'domain': 'en.wikipedia.org'
        },
        'type': 'edit',
        'title': 'Climate Change',
        'user': 'AITestUser',
        'bot': False,
        'timestamp': 1705320000,
        'namespace': 0,
        'wiki': 'enwiki',
        'comment': 'As an AI language model, I updated this article with information. '
                   'However, my knowledge was last updated in 2023.',
        'parsedcomment': 'Updated article with **important** information',
        'length': {'old': 20000, 'new': 20500},
        'revision': {'old': 789012, 'new': 789013}
    }

    print("\n" + "="*60)
    print("Processing event with AI indicators:")
    print(f"  Title: {ai_event['title']}")
    print(f"  Comment: {ai_event['comment'][:80]}...")
    print()

    # Detect AI content
    ai_flags = detect_ai_indicators(ai_event)

    print("AI Detection Results:")
    if ai_flags:
        for flag_type, detected in ai_flags.items():
            status = "✓ DETECTED" if detected else "  Not detected"
            print(f"  {status}: {flag_type}")
    else:
        print("  No indicators detected")


def main():
    """Run all examples."""
    print("="*60)
    print("Wikipedia Edits Pipeline - Basic Usage Example")
    print("="*60)
    print()

    # Process normal event
    process_single_event()

    # Process AI-generated event
    process_ai_generated_event()

    print("\n" + "="*60)
    print("Example complete!")
    print("="*60)


if __name__ == '__main__':
    main()
