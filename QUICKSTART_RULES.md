# Quick Reference: Rule Updates

## Update Rules (Single Command)

```bash
python scripts/update_rules.py \
  --new-version 1.0.X \
  --changelog "Your detailed description of changes"
```

## Common Commands

```bash
# Update with automatic rescan
python scripts/update_rules.py --new-version 1.0.4 --changelog "..." --auto-rescan

# Preview changes (dry run)
python scripts/update_rules.py --dry-run --new-version 1.0.4 --changelog "..."

# Check version consistency
python scripts/update_rules.py --validate-only

# View changelog
python -c "from wiki_pipeline.version import get_changelog; print(get_changelog())"

# Manual rescan
python scripts/scan_existing_data.py
```

## What Gets Updated Automatically

✓ `src/wiki_pipeline/version.py` - Version number and changelog  
✓ `src/wiki_pipeline/detection_patterns.json` - Version field  
✓ `src/wiki_pipeline/rule_versions/v{old}.py` - Archive created  
✓ All imports automatically use centralized version  

## Files You Edit Manually

- `src/wiki_pipeline/detection_patterns.json` - Add/modify patterns
- `src/wiki_pipeline/ai_detection.py` - Add/modify detection logic

## Security Features

- Input sanitization (version format, changelog length)
- SQL injection prevention (parameterized queries)
- Path traversal protection
- Pre-commit validation hook

## Full Documentation

See `docs/RULE_UPDATE_WORKFLOW.md` for complete details.
