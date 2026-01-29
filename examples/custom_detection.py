#!/usr/bin/env python3
"""Example showing how to add custom AI detection patterns.

This example demonstrates:
1. Loading existing detection patterns
2. Adding custom patterns
3. Testing custom detection logic
"""

import json
from pathlib import Path
from wiki_pipeline.ai_detection import load_detection_patterns, detect_ai_indicators


def load_custom_patterns():
    """Load and display current detection patterns."""
    patterns = load_detection_patterns()

    print("Current Detection Patterns:")
    print(f"  Version: {patterns['version']}")
    print(f"  Total categories: {len(patterns['patterns'])}")
    print()

    for category, config in patterns['patterns'].items():
        pattern_type = config['type']
        count = len(config['patterns'])
        print(f"  - {category}: {count} {pattern_type} patterns")

    return patterns


def create_custom_pattern_file():
    """Create a custom detection patterns file with additional rules."""

    # Load existing patterns
    base_patterns = load_detection_patterns()

    # Add custom patterns
    custom_patterns = base_patterns.copy()
    custom_patterns['version'] = '1.0.3-custom'

    # Example: Add custom spam detection patterns
    custom_patterns['patterns']['spam_indicators'] = {
        'type': 'phrase',
        'patterns': [
            'click here now',
            'limited time offer',
            'buy now',
            'earn money fast',
            'guaranteed results'
        ]
    }

    # Example: Add custom promotional patterns
    custom_patterns['patterns']['self_promotion'] = {
        'type': 'phrase',
        'patterns': [
            'visit my website',
            'check out my channel',
            'follow me on',
            'my latest product'
        ]
    }

    # Save to custom file
    custom_file = Path('detection_patterns_custom.json')
    with open(custom_file, 'w') as f:
        json.dump(custom_patterns, f, indent=2)

    print(f"\nCustom patterns file created: {custom_file}")
    print(f"Added categories:")
    print("  - spam_indicators (5 patterns)")
    print("  - self_promotion (4 patterns)")

    return custom_patterns


def test_custom_detection():
    """Test custom detection patterns."""

    print("\n" + "="*60)
    print("Testing Custom Detection Patterns")
    print("="*60)

    # Test events
    test_events = [
        {
            'comment': 'Updated article with new information',
            'parsedcomment': 'Updated article',
            'title': 'Test Article',
            'user': 'RegularUser'
        },
        {
            'comment': 'Click here now for guaranteed results! Visit my website for more.',
            'parsedcomment': 'Click here now',
            'title': 'Spam Article',
            'user': 'SpamUser'
        },
        {
            'comment': 'Check out my channel for more content. Follow me on social media!',
            'parsedcomment': 'Check out my channel',
            'title': 'Promotional Article',
            'user': 'PromoUser'
        }
    ]

    for i, event in enumerate(test_events, 1):
        print(f"\nTest Event {i}:")
        print(f"  Comment: {event['comment'][:60]}...")

        # Note: detect_ai_indicators uses the default patterns file
        # To use custom patterns, you would need to modify the function
        # or create a new detection function
        flags = detect_ai_indicators(event)

        if any(flags.values()):
            print("  Detection results:")
            for flag_type, detected in flags.items():
                if detected:
                    print(f"    ✓ {flag_type}")
        else:
            print("  No indicators detected")


def demonstrate_pattern_matching():
    """Demonstrate how pattern matching works."""

    print("\n" + "="*60)
    print("Pattern Matching Demonstration")
    print("="*60)

    # Load patterns
    patterns = load_detection_patterns()

    # Example text with various AI indicators
    test_texts = {
        'chatgpt_artifacts': "The response contains oaicite:123 references",
        'prompt_refusal': "As an AI language model, I cannot provide that information",
        'markdown_syntax': "This is **bold** and this is _italic_ text",
        'emojis': "Great article! 🎉 Very informative 📚",
        'collaborative_communication': "I'd be happy to help you with that question",
        'knowledge_cutoff': "My training data only goes up to 2023",
        'clean_text': "This is a normal Wikipedia edit with no AI indicators"
    }

    for category, text in test_texts.items():
        print(f"\n{category}:")
        print(f"  Text: {text}")

        # Simple pattern matching (for demonstration)
        text_lower = text.lower()
        detected = []

        for pattern_name, pattern_config in patterns['patterns'].items():
            if pattern_config['type'] == 'phrase':
                for pattern in pattern_config['patterns']:
                    if pattern.lower() in text_lower:
                        detected.append(pattern_name)
                        break

        if detected:
            print(f"  Detected: {', '.join(detected)}")
        else:
            print("  Detected: None")


def main():
    """Run all examples."""
    print("="*60)
    print("Custom AI Detection Patterns Example")
    print("="*60)
    print()

    # Load and display current patterns
    load_custom_patterns()

    # Create custom pattern file
    create_custom_pattern_file()

    # Test detection
    test_custom_detection()

    # Demonstrate pattern matching
    demonstrate_pattern_matching()

    print("\n" + "="*60)
    print("Example complete!")
    print("\nTo use custom patterns in production:")
    print("1. Edit detection_patterns.json")
    print("2. Increment the version number")
    print("3. Add your patterns to the appropriate category")
    print("4. Test thoroughly before deploying")
    print("="*60)


if __name__ == '__main__':
    main()
