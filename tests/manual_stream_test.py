#!/usr/bin/env python3
"""Test Wikimedia SSE stream connection without Pub/Sub."""
import json
import requests
from datetime import datetime

def test_wikimedia_stream():
    """Test if we can receive data from Wikimedia SSE."""
    url = 'https://stream.wikimedia.org/v2/stream/recentchange'
    headers = {
        'Accept': 'text/event-stream',
        'Cache-Control': 'no-cache',
        'User-Agent': 'WikiEditsPipeline/1.0 (aglines@example.com) Python/requests'
    }
    
    print("🌐 Testing Wikimedia SSE stream connection...")
    print(f"URL: {url}")
    
    try:
        response = requests.get(url, headers=headers, stream=True, timeout=30)
        response.raise_for_status()
        
        print("✅ Connected to Wikimedia stream")
        event_count = 0
        max_events = 5  # Just test a few events
        
        for line in response.iter_lines(decode_unicode=True):
            if line.startswith('data: '):
                event_data = line[6:]  # Remove 'data: ' prefix
                if event_data:
                    try:
                        data = json.loads(event_data)
                        
                        # Skip canary events
                        if data.get('meta', {}).get('domain') == 'canary':
                            print("🔍 Skipping canary event")
                            continue
                        
                        event_count += 1
                        print(f"📨 Event {event_count}: {data.get('type', 'unknown')} on {data.get('wiki', 'unknown')} - {data.get('title', 'N/A')[:50]}")
                        
                        if event_count >= max_events:
                            print(f"✅ Successfully received {event_count} events from Wikimedia")
                            break
                            
                    except json.JSONDecodeError as e:
                        print(f"❌ Failed to parse event: {e}")
                    
    except requests.RequestException as e:
        print(f"❌ Stream connection failed: {e}")
        return False
    except KeyboardInterrupt:
        print("⏹️  Stream test stopped by user")
    
    return event_count > 0

if __name__ == '__main__':
    success = test_wikimedia_stream()
    if success:
        print("\n🎉 Wikimedia stream is working! Issue is likely with Pub/Sub setup.")
    else:
        print("\n❌ Wikimedia stream connection failed.")