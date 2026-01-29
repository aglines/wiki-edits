"""Unit tests for version management."""
import pytest

from wiki_pipeline.version import (
    get_version,
    get_changelog,
    validate_version_format,
    DETECTION_RULE_VERSION,
    RULE_CHANGELOG,
)


class TestGetVersion:
    """Tests for get_version function."""

    def test_returns_string(self):
        """Should return a string."""
        version = get_version()
        assert isinstance(version, str)

    def test_returns_current_version(self):
        """Should return current version constant."""
        version = get_version()
        assert version == DETECTION_RULE_VERSION

    def test_version_not_empty(self):
        """Version should not be empty."""
        version = get_version()
        assert len(version) > 0


class TestGetChangelog:
    """Tests for get_changelog function."""

    def test_get_all_changelogs(self):
        """Should return all changelogs when no version specified."""
        changelog = get_changelog()
        assert isinstance(changelog, str)
        assert len(changelog) > 0
        assert 'Detection Rule Changelog' in changelog

    def test_get_specific_version_changelog(self):
        """Should return changelog for specific version."""
        # Get first version from changelog
        version = list(RULE_CHANGELOG.keys())[0]
        changelog = get_changelog(version)
        assert isinstance(changelog, str)
        assert version in changelog

    def test_get_nonexistent_version(self):
        """Should handle nonexistent version gracefully."""
        changelog = get_changelog('99.99.99')
        assert 'No changelog found' in changelog

    def test_changelog_includes_all_versions(self):
        """Full changelog should include all versions."""
        changelog = get_changelog()
        for version in RULE_CHANGELOG.keys():
            assert version in changelog

    def test_changelog_includes_dates(self):
        """Changelog should include dates."""
        changelog = get_changelog()
        for entry in RULE_CHANGELOG.values():
            assert entry['date'] in changelog

    def test_specific_version_format(self):
        """Specific version changelog should have correct format."""
        version = list(RULE_CHANGELOG.keys())[0]
        changelog = get_changelog(version)
        assert f"v{version}" in changelog
        assert RULE_CHANGELOG[version]['date'] in changelog
        assert RULE_CHANGELOG[version]['changes'] in changelog


class TestValidateVersionFormat:
    """Tests for validate_version_format function."""

    def test_valid_version_format(self):
        """Valid semantic versions should pass."""
        assert validate_version_format('1.0.0') is True
        assert validate_version_format('0.1.0') is True
        assert validate_version_format('10.20.30') is True
        assert validate_version_format('1.2.3') is True

    def test_invalid_version_format(self):
        """Invalid versions should fail."""
        assert validate_version_format('1.0') is False
        assert validate_version_format('1') is False
        assert validate_version_format('v1.0.0') is False
        assert validate_version_format('1.0.0-beta') is False
        assert validate_version_format('1.0.0.0') is False
        assert validate_version_format('') is False
        assert validate_version_format('invalid') is False

    def test_version_with_letters(self):
        """Versions with letters should fail."""
        assert validate_version_format('1.a.0') is False
        assert validate_version_format('a.b.c') is False

    def test_version_with_spaces(self):
        """Versions with spaces should fail."""
        assert validate_version_format('1. 0. 0') is False
        assert validate_version_format(' 1.0.0') is False

    def test_current_version_is_valid(self):
        """Current version constant should be valid."""
        assert validate_version_format(DETECTION_RULE_VERSION) is True


class TestRuleChangelog:
    """Tests for RULE_CHANGELOG constant."""

    def test_changelog_is_dict(self):
        """RULE_CHANGELOG should be a dictionary."""
        assert isinstance(RULE_CHANGELOG, dict)

    def test_changelog_not_empty(self):
        """RULE_CHANGELOG should not be empty."""
        assert len(RULE_CHANGELOG) > 0

    def test_all_versions_have_date(self):
        """All changelog entries should have date."""
        for version, entry in RULE_CHANGELOG.items():
            assert 'date' in entry
            assert isinstance(entry['date'], str)

    def test_all_versions_have_changes(self):
        """All changelog entries should have changes."""
        for version, entry in RULE_CHANGELOG.items():
            assert 'changes' in entry
            assert isinstance(entry['changes'], str)
            assert len(entry['changes']) > 0

    def test_current_version_in_changelog(self):
        """Current version should be in changelog."""
        assert DETECTION_RULE_VERSION in RULE_CHANGELOG

    def test_all_versions_valid_format(self):
        """All versions in changelog should have valid format."""
        for version in RULE_CHANGELOG.keys():
            assert validate_version_format(version) is True
