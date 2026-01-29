"""Unit tests for AI detection logic."""
import pytest
from unittest.mock import patch, MagicMock

from wiki_pipeline.ai_detection import (
    detect_ai_indicators,
    load_detection_patterns,
    load_emoji_list,
    get_patterns,
)


@pytest.fixture
def sample_content():
    """Sample content for testing."""
    return {
        'comment': 'Updated article with new information',
        'parsed_comment': '<p>Updated article</p>',
        'title': 'Test Article',
        'user': 'TestUser',
        'len_old': 100,
        'len_new': 150,
        'minor': False,
        'bot': False
    }


class TestDetectAIIndicators:
    """Tests for detect_ai_indicators function."""

    def test_no_ai_indicators(self, sample_content):
        """Clean content should return no flags."""
        flags = detect_ai_indicators(sample_content)
        assert isinstance(flags, dict)
        assert len(flags) == 0

    def test_chatgpt_artifacts(self, sample_content):
        """Should detect ChatGPT artifacts."""
        sample_content['comment'] = 'Check this link utm_source=chatgpt.com for details'
        flags = detect_ai_indicators(sample_content)
        assert 'chatgpt_artifacts' in flags

    def test_chatgpt_attribution(self, sample_content):
        """Should detect ChatGPT attribution."""
        sample_content['comment'] = 'Source: {"attribution":{"attributableindex":1}'
        flags = detect_ai_indicators(sample_content)
        assert 'chatgpt_attribution' in flags

    def test_prompt_refusal(self, sample_content):
        """Should detect prompt refusal patterns."""
        sample_content['comment'] = 'As an AI language model, I cannot assist'
        flags = detect_ai_indicators(sample_content)
        assert 'prompt_refusal' in flags

    def test_markdown_syntax(self, sample_content):
        """Should detect markdown syntax."""
        sample_content['comment'] = 'Updated with **bold text** and *italic*'
        flags = detect_ai_indicators(sample_content)
        assert 'markdown_syntax' in flags

    def test_excessive_em_dashes(self, sample_content):
        """Should detect excessive em-dashes."""
        sample_content['comment'] = 'This is a test — and another — and more — dashes'
        flags = detect_ai_indicators(sample_content)
        assert 'excessive_em_dashes' in flags

    def test_promotional_language(self, sample_content):
        """Should detect promotional language."""
        sample_content['comment'] = 'This breathtaking location offers stunning views'
        flags = detect_ai_indicators(sample_content)
        assert 'promotional_language' in flags

    def test_importance_emphasis(self, sample_content):
        """Should detect importance emphasis."""
        sample_content['comment'] = 'This event plays a vital role in history'
        flags = detect_ai_indicators(sample_content)
        assert 'importance_emphasis' in flags

    def test_placeholder_templates(self, sample_content):
        """Should detect placeholder templates."""
        sample_content['comment'] = "Added [entertainer's name] to the cast list"
        flags = detect_ai_indicators(sample_content)
        assert 'placeholder_templates' in flags

    def test_knowledge_cutoff_disclaimers(self, sample_content):
        """Should detect knowledge cutoff disclaimers."""
        sample_content['comment'] = 'Up to my last training update, this was accurate'
        flags = detect_ai_indicators(sample_content)
        assert 'knowledge_cutoff_disclaimers' in flags

    def test_collaborative_communication(self, sample_content):
        """Should detect collaborative communication patterns."""
        sample_content['comment'] = 'I hope this helps with your research'
        flags = detect_ai_indicators(sample_content)
        assert 'collaborative_communication' in flags

    def test_case_insensitive_matching(self, sample_content):
        """Detection should be case-insensitive."""
        sample_content['comment'] = 'AS AN AI LANGUAGE MODEL'
        flags = detect_ai_indicators(sample_content)
        assert len(flags) > 0

    def test_multiple_flags(self, sample_content):
        """Should detect multiple indicators."""
        sample_content['comment'] = 'As an AI language model with **bold text**, I hope this helps'
        flags = detect_ai_indicators(sample_content)
        assert len(flags) >= 2

    def test_empty_content(self):
        """Empty content should not crash."""
        content = {
            'comment': '',
            'parsed_comment': '',
            'title': '',
            'user': '',
            'len_old': 0,
            'len_new': 0,
            'minor': False,
            'bot': False
        }
        flags = detect_ai_indicators(content)
        assert isinstance(flags, dict)

    def test_none_values(self):
        """None values should be handled gracefully."""
        content = {
            'comment': None,
            'parsed_comment': None,
            'title': None,
            'user': 'TestUser',
            'len_old': 0,
            'len_new': 0,
            'minor': False,
            'bot': False
        }
        flags = detect_ai_indicators(content)
        assert isinstance(flags, dict)

    def test_unicode_content(self, sample_content):
        """Should handle unicode characters."""
        sample_content['comment'] = 'Updated with émojis 🎉 and unicode'
        flags = detect_ai_indicators(sample_content)
        assert isinstance(flags, dict)


class TestLoadDetectionPatterns:
    """Tests for pattern loading."""

    def test_load_patterns_returns_dict(self):
        """Should return a dictionary."""
        patterns = load_detection_patterns()
        assert isinstance(patterns, dict)

    def test_patterns_has_patterns_key(self):
        """Should have 'patterns' key."""
        patterns = load_detection_patterns()
        assert 'patterns' in patterns

    def test_get_patterns(self):
        """get_patterns should return patterns dict."""
        patterns = get_patterns()
        assert isinstance(patterns, dict)

    @patch('builtins.open', side_effect=FileNotFoundError)
    def test_load_patterns_file_not_found(self, mock_open):
        """Should handle missing patterns file gracefully."""
        # Clear cache first
        load_detection_patterns.cache_clear()
        patterns = load_detection_patterns()
        assert isinstance(patterns, dict)
        assert 'patterns' in patterns


class TestLoadEmojiList:
    """Tests for emoji list loading."""

    def test_load_emoji_list_returns_set(self):
        """Should return a set."""
        emojis = load_emoji_list()
        assert isinstance(emojis, set)

    @patch('builtins.open', side_effect=FileNotFoundError)
    def test_load_emoji_list_file_not_found(self, mock_open):
        """Should handle missing emoji file gracefully."""
        # Clear cache first
        load_emoji_list.cache_clear()
        emojis = load_emoji_list()
        assert isinstance(emojis, set)
        assert len(emojis) == 0
