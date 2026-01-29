#!/usr/bin/env python3
"""
Orchestration script for updating detection rules and managing versions.

This script automates the entire rule update workflow:
1. Validates new version format
2. Archives current version
3. Updates version.py with new version and changelog
4. Updates detection_patterns.json version
5. Optionally triggers rescan of existing data
6. Validates all changes

Usage:
    python scripts/update_rules.py --new-version 1.0.4 --changelog "Added new patterns for X"
    python scripts/update_rules.py --new-version 1.0.4 --changelog "..." --auto-rescan
    python scripts/update_rules.py --validate-only  # Check version consistency
    python scripts/update_rules.py --dry-run --new-version 1.0.4 --changelog "..."
"""

import os
import sys
import re
import json
import shutil
import argparse
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

# Color codes for terminal output
class Colors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'


def sanitize_version(version: str) -> str:
    """Sanitize and validate version string."""
    version = version.strip()
    if not re.match(r'^\d+\.\d+\.\d+$', version):
        raise ValueError(f"Invalid version format: {version}. Expected format: X.Y.Z")
    return version


def sanitize_changelog(changelog: str) -> str:
    """Sanitize changelog text to prevent injection attacks."""
    if not changelog or not changelog.strip():
        raise ValueError("Changelog cannot be empty")
    
    # Remove any control characters
    changelog = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', changelog)
    
    # Limit length
    if len(changelog) > 1000:
        raise ValueError("Changelog too long (max 1000 characters)")
    
    return changelog.strip()


def get_project_root() -> Path:
    """Get project root directory."""
    return Path(__file__).parent.parent


def get_current_version() -> str:
    """Read current version from version.py."""
    try:
        from wiki_pipeline.version import DETECTION_RULE_VERSION
        return DETECTION_RULE_VERSION
    except ImportError as e:
        raise RuntimeError(f"Failed to import current version: {e}")


def validate_version_consistency() -> Tuple[bool, list]:
    """
    Validate that version numbers are consistent across all files.
    
    Returns:
        Tuple of (is_valid, list_of_issues)
    """
    issues = []
    project_root = get_project_root()
    
    try:
        current_version = get_current_version()
    except Exception as e:
        issues.append(f"Cannot read version from version.py: {e}")
        return False, issues
    
    # Check detection_patterns.json
    patterns_file = project_root / "src" / "wiki_pipeline" / "detection_patterns.json"
    if patterns_file.exists():
        try:
            with open(patterns_file, 'r') as f:
                patterns_data = json.load(f)
                json_version = patterns_data.get('version', '')
                if json_version != current_version:
                    issues.append(
                        f"Version mismatch: version.py has {current_version}, "
                        f"detection_patterns.json has {json_version}"
                    )
        except (json.JSONDecodeError, IOError) as e:
            issues.append(f"Failed to read detection_patterns.json: {e}")
    else:
        issues.append("detection_patterns.json not found")
    
    return len(issues) == 0, issues


def archive_current_version(dry_run: bool = False) -> bool:
    """
    Archive current ai_detection.py to rule_versions directory.
    
    Args:
        dry_run: If True, only show what would be done
        
    Returns:
        True if successful, False otherwise
    """
    project_root = get_project_root()
    current_version = get_current_version()
    
    source_file = project_root / "src" / "wiki_pipeline" / "ai_detection.py"
    versions_dir = project_root / "src" / "wiki_pipeline" / "rule_versions"
    archive_file = versions_dir / f"v{current_version}.py"
    
    if not source_file.exists():
        print(f"{Colors.FAIL}✗ Source file not found: {source_file}{Colors.ENDC}")
        return False
    
    if archive_file.exists():
        print(f"{Colors.WARNING}⚠ Archive already exists: {archive_file}{Colors.ENDC}")
        print(f"  Skipping archival (version {current_version} already archived)")
        return True
    
    if dry_run:
        print(f"{Colors.OKCYAN}[DRY RUN] Would archive:{Colors.ENDC}")
        print(f"  {source_file} → {archive_file}")
        return True
    
    try:
        versions_dir.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source_file, archive_file)
        print(f"{Colors.OKGREEN}✓ Archived v{current_version} to {archive_file}{Colors.ENDC}")
        return True
    except IOError as e:
        print(f"{Colors.FAIL}✗ Failed to archive: {e}{Colors.ENDC}")
        return False


def update_version_file(new_version: str, changelog: str, dry_run: bool = False) -> bool:
    """
    Update version.py with new version and changelog entry.
    
    Args:
        new_version: New version string (X.Y.Z)
        changelog: Changelog text for this version
        dry_run: If True, only show what would be done
        
    Returns:
        True if successful, False otherwise
    """
    project_root = get_project_root()
    version_file = project_root / "src" / "wiki_pipeline" / "version.py"
    
    if not version_file.exists():
        print(f"{Colors.FAIL}✗ version.py not found: {version_file}{Colors.ENDC}")
        return False
    
    try:
        with open(version_file, 'r') as f:
            content = f.read()
        
        # Update DETECTION_RULE_VERSION
        new_content = re.sub(
            r'DETECTION_RULE_VERSION = "[^"]+"',
            f'DETECTION_RULE_VERSION = "{new_version}"',
            content
        )
        
        # Add new changelog entry at the beginning of RULE_CHANGELOG dict
        today = datetime.now().strftime("%Y-%m-%d")
        new_entry = f'''    "{new_version}": {{
        "date": "{today}",
        "changes": "{changelog}"
    }},'''
        
        # Find the RULE_CHANGELOG dict and insert after the opening brace
        new_content = re.sub(
            r'(RULE_CHANGELOG: Dict\[str, Dict\[str, str\]\] = \{)\n',
            f'\\1\n{new_entry}\n',
            new_content
        )
        
        if dry_run:
            print(f"{Colors.OKCYAN}[DRY RUN] Would update version.py:{Colors.ENDC}")
            print(f"  Version: {get_current_version()} → {new_version}")
            print(f"  Changelog: {changelog}")
            return True
        
        with open(version_file, 'w') as f:
            f.write(new_content)
        
        print(f"{Colors.OKGREEN}✓ Updated version.py to v{new_version}{Colors.ENDC}")
        return True
        
    except (IOError, re.error) as e:
        print(f"{Colors.FAIL}✗ Failed to update version.py: {e}{Colors.ENDC}")
        return False


def update_patterns_json(new_version: str, dry_run: bool = False) -> bool:
    """
    Update detection_patterns.json with new version.
    
    Args:
        new_version: New version string (X.Y.Z)
        dry_run: If True, only show what would be done
        
    Returns:
        True if successful, False otherwise
    """
    project_root = get_project_root()
    patterns_file = project_root / "src" / "wiki_pipeline" / "detection_patterns.json"
    
    if not patterns_file.exists():
        print(f"{Colors.FAIL}✗ detection_patterns.json not found: {patterns_file}{Colors.ENDC}")
        return False
    
    try:
        with open(patterns_file, 'r') as f:
            patterns_data = json.load(f)
        
        old_version = patterns_data.get('version', 'unknown')
        patterns_data['version'] = new_version
        
        if dry_run:
            print(f"{Colors.OKCYAN}[DRY RUN] Would update detection_patterns.json:{Colors.ENDC}")
            print(f"  Version: {old_version} → {new_version}")
            return True
        
        with open(patterns_file, 'w') as f:
            json.dump(patterns_data, f, indent=2)
            f.write('\n')  # Add trailing newline
        
        print(f"{Colors.OKGREEN}✓ Updated detection_patterns.json to v{new_version}{Colors.ENDC}")
        return True
        
    except (json.JSONDecodeError, IOError) as e:
        print(f"{Colors.FAIL}✗ Failed to update detection_patterns.json: {e}{Colors.ENDC}")
        return False


def trigger_rescan(dry_run: bool = False) -> bool:
    """
    Trigger scan_existing_data.py to rescan with new rules.
    
    Args:
        dry_run: If True, only show what would be done
        
    Returns:
        True if successful, False otherwise
    """
    project_root = get_project_root()
    scan_script = project_root / "scripts" / "scan_existing_data.py"
    
    if not scan_script.exists():
        print(f"{Colors.FAIL}✗ scan_existing_data.py not found: {scan_script}{Colors.ENDC}")
        return False
    
    if dry_run:
        print(f"{Colors.OKCYAN}[DRY RUN] Would execute:{Colors.ENDC}")
        print(f"  python {scan_script}")
        return True
    
    print(f"{Colors.OKBLUE}→ Starting rescan with new rules...{Colors.ENDC}")
    print(f"  (This may take a while depending on data volume)")
    
    try:
        result = subprocess.run(
            [sys.executable, str(scan_script)],
            cwd=project_root,
            capture_output=False,
            text=True
        )
        
        if result.returncode == 0:
            print(f"{Colors.OKGREEN}✓ Rescan completed successfully{Colors.ENDC}")
            return True
        else:
            print(f"{Colors.FAIL}✗ Rescan failed with exit code {result.returncode}{Colors.ENDC}")
            return False
            
    except subprocess.SubprocessError as e:
        print(f"{Colors.FAIL}✗ Failed to run rescan: {e}{Colors.ENDC}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Update detection rules and manage versions",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Update to new version with changelog
  python scripts/update_rules.py --new-version 1.0.4 --changelog "Added detection for new AI patterns"
  
  # Update and automatically trigger rescan
  python scripts/update_rules.py --new-version 1.0.4 --changelog "..." --auto-rescan
  
  # Dry run to see what would happen
  python scripts/update_rules.py --dry-run --new-version 1.0.4 --changelog "..."
  
  # Validate version consistency only
  python scripts/update_rules.py --validate-only
        """
    )
    
    parser.add_argument(
        '--new-version',
        type=str,
        help='New version number (format: X.Y.Z)'
    )
    
    parser.add_argument(
        '--changelog',
        type=str,
        help='Free-text description of changes in this version'
    )
    
    parser.add_argument(
        '--auto-rescan',
        action='store_true',
        help='Automatically trigger scan_existing_data.py after updating'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be done without making changes'
    )
    
    parser.add_argument(
        '--validate-only',
        action='store_true',
        help='Only validate version consistency across files'
    )
    
    args = parser.parse_args()
    
    # Handle validate-only mode
    if args.validate_only:
        print(f"{Colors.HEADER}=== Validating Version Consistency ==={Colors.ENDC}\n")
        is_valid, issues = validate_version_consistency()
        
        if is_valid:
            current_version = get_current_version()
            print(f"{Colors.OKGREEN}✓ All version numbers are consistent: v{current_version}{Colors.ENDC}")
            return 0
        else:
            print(f"{Colors.FAIL}✗ Version inconsistencies found:{Colors.ENDC}")
            for issue in issues:
                print(f"  • {issue}")
            return 1
    
    # Validate required arguments for update mode
    if not args.new_version or not args.changelog:
        parser.error("--new-version and --changelog are required (unless using --validate-only)")
    
    # Sanitize inputs
    try:
        new_version = sanitize_version(args.new_version)
        changelog = sanitize_changelog(args.changelog)
    except ValueError as e:
        print(f"{Colors.FAIL}✗ Invalid input: {e}{Colors.ENDC}")
        return 1
    
    # Get current version
    try:
        current_version = get_current_version()
    except RuntimeError as e:
        print(f"{Colors.FAIL}✗ {e}{Colors.ENDC}")
        return 1
    
    # Display header
    mode = "DRY RUN" if args.dry_run else "UPDATING"
    print(f"{Colors.HEADER}=== {mode}: Detection Rules ==={Colors.ENDC}\n")
    print(f"Current version: {Colors.BOLD}{current_version}{Colors.ENDC}")
    print(f"New version:     {Colors.BOLD}{new_version}{Colors.ENDC}")
    print(f"Changelog:       {changelog}\n")
    
    # Validate version progression
    if new_version <= current_version:
        print(f"{Colors.WARNING}⚠ Warning: New version ({new_version}) is not greater than current version ({current_version}){Colors.ENDC}")
        if not args.dry_run:
            response = input("Continue anyway? (y/N): ")
            if response.lower() != 'y':
                print("Aborted.")
                return 1
    
    # Execute workflow steps
    success = True
    
    # Step 1: Archive current version
    print(f"{Colors.HEADER}Step 1: Archive current version{Colors.ENDC}")
    if not archive_current_version(args.dry_run):
        success = False
    print()
    
    # Step 2: Update version.py
    if success:
        print(f"{Colors.HEADER}Step 2: Update version.py{Colors.ENDC}")
        if not update_version_file(new_version, changelog, args.dry_run):
            success = False
        print()
    
    # Step 3: Update detection_patterns.json
    if success:
        print(f"{Colors.HEADER}Step 3: Update detection_patterns.json{Colors.ENDC}")
        if not update_patterns_json(new_version, args.dry_run):
            success = False
        print()
    
    # Step 4: Validate consistency
    if success and not args.dry_run:
        print(f"{Colors.HEADER}Step 4: Validate consistency{Colors.ENDC}")
        is_valid, issues = validate_version_consistency()
        if not is_valid:
            print(f"{Colors.FAIL}✗ Validation failed:{Colors.ENDC}")
            for issue in issues:
                print(f"  • {issue}")
            success = False
        else:
            print(f"{Colors.OKGREEN}✓ All versions are consistent{Colors.ENDC}")
        print()
    
    # Step 5: Optional rescan
    if success and args.auto_rescan:
        print(f"{Colors.HEADER}Step 5: Trigger rescan{Colors.ENDC}")
        if not trigger_rescan(args.dry_run):
            print(f"{Colors.WARNING}⚠ Rescan failed, but version update was successful{Colors.ENDC}")
        print()
    
    # Summary
    print(f"{Colors.HEADER}=== Summary ==={Colors.ENDC}")
    if success:
        if args.dry_run:
            print(f"{Colors.OKGREEN}✓ Dry run completed successfully{Colors.ENDC}")
            print(f"  Run without --dry-run to apply changes")
        else:
            print(f"{Colors.OKGREEN}✓ Successfully updated to v{new_version}{Colors.ENDC}")
            if not args.auto_rescan:
                print(f"\n{Colors.OKBLUE}Next step:{Colors.ENDC}")
                print(f"  Run: python scripts/scan_existing_data.py")
                print(f"  to rescan existing data with new rules")
        return 0
    else:
        print(f"{Colors.FAIL}✗ Update failed{Colors.ENDC}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
