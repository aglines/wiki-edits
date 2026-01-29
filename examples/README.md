# Examples

This directory contains usage examples for the Wikipedia Edits Pipeline.

## Running Examples

All examples should be run from the project root directory using `uv run`:

```bash
# From project root
uv run python examples/basic_usage.py
uv run python examples/custom_detection.py
```

## Available Examples

### 1. `basic_usage.py`

Demonstrates fundamental pipeline operations:
- Processing a single Wikipedia edit event
- Extracting and normalizing fields
- Running AI content detection
- Understanding detection results

**Run:**
```bash
uv run python examples/basic_usage.py
```

**Expected Output:**
- Field extraction results
- AI detection flags
- Comparison of normal vs AI-generated content

---

### 2. `custom_detection.py`

Shows how to customize AI detection:
- Loading existing detection patterns
- Adding custom pattern categories
- Creating custom detection rules
- Testing pattern matching

**Run:**
```bash
uv run python examples/custom_detection.py
```

**Expected Output:**
- Current pattern categories
- Custom pattern file creation
- Pattern matching demonstrations

**To use custom patterns:**
1. Edit `detection_patterns.json`
2. Add new pattern categories or modify existing ones
3. Increment the version number
4. Redeploy the pipeline

---

## Querying Results in BigQuery

After running the pipeline, you can analyze results using SQL queries in the BigQuery console. Example queries:

```sql
-- Total events processed
SELECT COUNT(*) FROM `project.dataset.wiki_edits`;

-- AI-flagged events
SELECT * FROM `project.dataset.wiki_edits_ai_events`
ORDER BY detection_timestamp DESC
LIMIT 100;

-- Find users with most AI-flagged content
SELECT
  user,
  COUNT(*) as ai_flagged_count
FROM `project.dataset.wiki_edits_ai_events`
GROUP BY user
ORDER BY ai_flagged_count DESC
LIMIT 10;
```

Replace `project.dataset` with your actual GCP project and dataset names.

---

## Example Data

### Sample Wikipedia Event

```json
{
  "meta": {
    "id": "abc123",
    "dt": "2024-01-15T10:30:00Z",
    "domain": "en.wikipedia.org"
  },
  "type": "edit",
  "title": "Machine Learning",
  "user": "ExampleUser",
  "bot": false,
  "timestamp": 1705318200,
  "namespace": 0,
  "wiki": "enwiki",
  "minor": false,
  "patrolled": true,
  "length": {"old": 15000, "new": 15250},
  "revision": {"old": 123456, "new": 123457},
  "comment": "Updated information about neural networks"
}
```

### Sample AI Detection Output

```json
{
  "chatgpt_artifacts": false,
  "chatgpt_attribution": false,
  "prompt_refusal": true,
  "emojis": false,
  "markdown_syntax": true,
  "knowledge_cutoff_disclaimers": true,
  "collaborative_communication": true
}
```

## Next Steps

After running the examples:

1. **Test locally**: `uv run python main.py local --max-events 20`
2. **Stream real data**: `uv run python main.py stream --max-events 1000`
3. **Deploy to Dataflow**: `uv run python main.py pipeline`
4. **Query results**: Use the SQL examples in BigQuery

## Troubleshooting

**Import errors:**
```bash
# Ensure package is installed
uv sync
```

**Missing dependencies:**
```bash
# Sync all dependencies
uv sync
```

**Can't find detection patterns:**
```bash
# Run from project root, not examples/
cd ..
uv run python examples/basic_usage.py
```

## Additional Resources

- [Main README](../README.md) - Full documentation
- [CONTRIBUTING.md](../CONTRIBUTING.md) - Development guide
- [SPEC.md](../SPEC.md) - Technical specification
