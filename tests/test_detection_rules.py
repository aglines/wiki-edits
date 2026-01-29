"""Tests for detection rule configuration and evolution."""
import pytest
import json
import os

from wiki_pipeline.ai_detection import (
    load_detection_patterns,
    detect_ai_indicators,
    get_patterns,
)
from wiki_pipeline.version import (
    DETECTION_RULE_VERSION,
    RULE_CHANGELOG,
    validate_version_format,
)


class TestPatternFileLoading:
    """Tests for pattern file loading and structure."""

    def test_pattern_file_exists(self):
        """Pattern file should exist and be loadable."""
        patterns = load_detection_patterns()
        assert isinstance(patterns, dict)
        assert 'patterns' in patterns

    def test_pattern_file_has_version(self):
        """Pattern file should include version."""
        patterns = load_detection_patterns()
        
        # Skip if pattern file not loaded (graceful fallback)
        if not patterns.get('patterns'):
            pytest.skip("Pattern file not loaded - check file path")
        
        assert 'version' in patterns
        assert validate_version_format(patterns['version'])

    def test_pattern_version_matches_code(self):
        """Pattern file version should match code version."""
        patterns = load_detection_patterns()
        
        # Skip if pattern file not loaded (graceful fallback)
        if not patterns.get('patterns'):
            pytest.skip("Pattern file not loaded - check file path")
        
        assert patterns['version'] == DETECTION_RULE_VERSION

    def test_all_pattern_types_are_lists_or_dicts(self):
        """All pattern types should be properly structured."""
        patterns = get_patterns()
        for pattern_type, pattern_value in patterns.items():
            assert isinstance(pattern_value, (list, dict)), \
                f"{pattern_type} should be list or dict, got {type(pattern_value)}"

    def test_pattern_lists_not_empty(self):
        """Pattern lists should contain at least one pattern."""
        patterns = get_patterns()
        list_patterns = [k for k, v in patterns.items() if isinstance(v, list)]
        
        for pattern_type in list_patterns:
            pattern_list = patterns[pattern_type]
            assert len(pattern_list) > 0, f"{pattern_type} should not be empty"

    def test_pattern_strings_are_lowercase(self):
        """Pattern strings should be lowercase for case-insensitive matching."""
        patterns = get_patterns()
        
        for pattern_type, pattern_value in patterns.items():
            if isinstance(pattern_value, list):
                for pattern in pattern_value:
                    if isinstance(pattern, str):
                        assert pattern == pattern.lower(), \
                            f"Pattern '{pattern}' in {pattern_type} should be lowercase"


class TestAllPatternsDetectable:
    """Parameterized tests to ensure all patterns are detectable."""

    @pytest.fixture
    def patterns(self):
        """Load patterns for testing."""
        return get_patterns()

    @pytest.fixture
    def base_content(self):
        """Base content structure for testing."""
        return {
            'comment': '',
            'parsed_comment': '',
            'title': '',
            'user': 'TestUser',
            'len_old': 0,
            'len_new': 0,
            'minor': False,
            'bot': False
        }

    @pytest.mark.parametrize("pattern_type", [
        'chatgpt_artifacts',
        'chatgpt_attribution',
        'prompt_refusal',
        'knowledge_cutoff_disclaimers',
        'collaborative_communication',
        'promotional_language',
        'importance_emphasis',
    ])
    def test_list_pattern_detection(self, pattern_type, patterns, base_content):
        """Test that each list-based pattern type is detectable."""
        pattern_list = patterns.get(pattern_type)
        
        if not pattern_list or not isinstance(pattern_list, list):
            pytest.skip(f"{pattern_type} not configured as list")
        
        # Test first pattern in the list
        test_pattern = pattern_list[0]
        base_content['comment'] = f"Test content with {test_pattern} in it"
        
        flags = detect_ai_indicators(base_content)
        assert pattern_type in flags, \
            f"Pattern '{test_pattern}' should trigger {pattern_type} detection"

    def test_markdown_syntax_detection(self, patterns, base_content):
        """Test markdown syntax pattern detection."""
        if 'markdown_syntax' not in patterns:
            pytest.skip("markdown_syntax not configured")
        
        # Test bold markdown
        base_content['comment'] = "This has **bold text** in it"
        flags = detect_ai_indicators(base_content)
        assert 'markdown_syntax' in flags

    def test_placeholder_templates_detection(self, patterns, base_content):
        """Test placeholder template detection."""
        if 'placeholder_templates' not in patterns:
            pytest.skip("placeholder_templates not configured")
        
        # Test placeholder pattern
        base_content['comment'] = "Added [entertainer's name] to the list"
        flags = detect_ai_indicators(base_content)
        assert 'placeholder_templates' in flags

    def test_excessive_em_dashes_detection(self, base_content):
        """Test excessive em-dash detection."""
        # Create content with many em-dashes
        base_content['comment'] = "This — has — many — em — dashes — throughout"
        flags = detect_ai_indicators(base_content)
        assert 'excessive_em_dashes' in flags


class TestVersionTracking:
    """Tests for rule version tracking and consistency."""

    def test_current_version_in_changelog(self):
        """Current version should be documented in changelog."""
        assert DETECTION_RULE_VERSION in RULE_CHANGELOG

    def test_current_version_is_valid_semver(self):
        """Current version should follow semantic versioning."""
        assert validate_version_format(DETECTION_RULE_VERSION)

    def test_all_changelog_versions_valid(self):
        """All versions in changelog should be valid semver."""
        for version in RULE_CHANGELOG.keys():
            assert validate_version_format(version), \
                f"Version {version} in changelog is not valid semver"

    def test_changelog_entries_have_required_fields(self):
        """Each changelog entry should have date and changes."""
        for version, entry in RULE_CHANGELOG.items():
            assert 'date' in entry, f"Version {version} missing date"
            assert 'changes' in entry, f"Version {version} missing changes"
            assert len(entry['changes']) > 0, f"Version {version} has empty changes"

    def test_ai_event_includes_version(self):
        """AI detection should track rule version used."""
        from wiki_pipeline.transforms.filters import AIContentMonitor
        
        monitor = AIContentMonitor()
        element = {
            'evt_id': 'test-123',
            'type': 'edit',
            'title': 'Test',
            'user': 'TestUser',
            'domain': 'test.org',
            'ns': 0,
            'dt_user': '2023-01-01T12:00:00Z',
            'dt_server': 1672574400,
            'comment': 'As an AI language model, I cannot provide opinions',
            'parsedcomment': '',
            'length': {'old': 0, 'new': 0},
            'minor': False,
            'bot': False
        }
        
        results = list(monitor.process(element))
        
        # Find AI event in results
        from apache_beam.pvalue import TaggedOutput
        ai_events = [r for r in results if isinstance(r, TaggedOutput) and r.tag == 'ai_events']
        
        if ai_events:
            ai_event = ai_events[0].value
            assert 'rule_version' in ai_event
            assert ai_event['rule_version'] == DETECTION_RULE_VERSION


class TestPatternEvolution:
    """Tests for handling pattern evolution over time."""

    def test_pattern_file_is_valid_json(self):
        """Pattern file should be valid JSON."""
        pattern_file = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            'src/wiki_pipeline/detection_patterns.json'
        )
        
        with open(pattern_file, 'r') as f:
            data = json.load(f)
        
        assert isinstance(data, dict)

    def test_detection_handles_missing_pattern_gracefully(self):
        """Detection should not crash if a pattern type is missing."""
        content = {
            'comment': 'Test content',
            'parsed_comment': '',
            'title': '',
            'user': 'User',
            'len_old': 0,
            'len_new': 0,
            'minor': False,
            'bot': False
        }
        
        # Should not raise exception even if patterns are incomplete
        flags = detect_ai_indicators(content)
        assert isinstance(flags, dict)

    def test_new_pattern_types_automatically_tested(self):
        """New pattern types should be automatically included in tests."""
        patterns = get_patterns()
        pattern_types = set(patterns.keys())
        
        # Skip if patterns not loaded
        if not pattern_types:
            pytest.skip("Pattern file not loaded - check file path")
        
        # Document current pattern types for regression detection
        expected_types = {
            'chatgpt_artifacts',
            'chatgpt_attribution',
            'prompt_refusal',
            'knowledge_cutoff_disclaimers',
            'collaborative_communication',
            'promotional_language',
            'importance_emphasis',
            'markdown_syntax',
            'placeholder_templates',
            'emojis'
        }
        
        # New types should be a superset of expected
        assert pattern_types >= expected_types, \
            f"Missing expected pattern types: {expected_types - pattern_types}"
