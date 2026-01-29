#!/usr/bin/env python3
"""
Download and parse Unicode emoji data to generate a local emoji list.

This script fetches the official Unicode emoji test file and extracts
all fully-qualified emojis into a JSON file for use in AI detection.
"""

import json
import re
import urllib.request
from pathlib import Path
from typing import List, Set

# Official Unicode emoji test data URL
EMOJI_TEST_URL = "https://unicode.org/Public/emoji/latest/emoji-test.txt"

def download_emoji_data() -> str:
    """Download the official Unicode emoji test file."""
    print(f"Downloading emoji data from {EMOJI_TEST_URL}...")
    with urllib.request.urlopen(EMOJI_TEST_URL) as response:
        return response.read().decode('utf-8')

def parse_emoji_data(data: str) -> Set[str]:
    """
    Parse emoji test file and extract fully-qualified emojis.
    
    The format is:
    # <codepoints> ; <status> # <emoji> <version> <description>
    
    We only want fully-qualified emojis (not component or unqualified).
    """
    emojis = set()
    
    # Pattern to match emoji lines
    # Example: 1F600 ; fully-qualified # 😀 E1.0 grinning face
    pattern = re.compile(r'^([0-9A-F ]+)\s*;\s*fully-qualified\s*#\s*(\S+)', re.MULTILINE)
    
    for match in pattern.finditer(data):
        emoji = match.group(2)
        emojis.add(emoji)
    
    return emojis

def save_emoji_list(emojis: Set[str], output_path: Path) -> None:
    """Save emoji list to JSON file."""
    # Sort for consistent output
    emoji_list = sorted(list(emojis))
    
    output_data = {
        "version": "unicode-15.1",
        "source": EMOJI_TEST_URL,
        "count": len(emoji_list),
        "emojis": emoji_list
    }
    
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, ensure_ascii=False, indent=2)
    
    print(f"✓ Saved {len(emoji_list)} emojis to {output_path}")

def main():
    """Main entry point."""
    # Determine output path relative to script location
    script_dir = Path(__file__).parent
    repo_root = script_dir.parent
    output_path = repo_root / "src" / "wiki_pipeline" / "assets" / "emoji.json"
    
    print("Unicode Emoji List Updater")
    print("=" * 50)
    
    try:
        # Download and parse
        data = download_emoji_data()
        emojis = parse_emoji_data(data)
        
        # Save to file
        save_emoji_list(emojis, output_path)
        
        print("\n✓ Emoji list updated successfully!")
        print(f"  Total emojis: {len(emojis)}")
        print(f"  Output: {output_path}")
        
    except Exception as e:
        print(f"\n✗ Error: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())
