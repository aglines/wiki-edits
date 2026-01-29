-- BigQuery SQL Examples for Wikipedia Edits Pipeline
-- Replace project.dataset with your actual project and dataset IDs

-- ============================================================================
-- Basic Queries
-- ============================================================================

-- Total events processed
SELECT COUNT(*) as total_events
FROM `project.dataset.wiki_edits`;

-- Events by type
SELECT
  type,
  COUNT(*) as count
FROM `project.dataset.wiki_edits`
GROUP BY type
ORDER BY count DESC;

-- Recent events (last 100)
SELECT
  evt_id,
  dt_user,
  type,
  title,
  user,
  domain
FROM `project.dataset.wiki_edits`
ORDER BY dt_user DESC
LIMIT 100;

-- ============================================================================
-- AI Detection Queries
-- ============================================================================

-- Total AI-flagged events
SELECT COUNT(*) as ai_flagged_events
FROM `project.dataset.wiki_edits_ai_events`;

-- AI events by detection type
SELECT
  flag_type,
  COUNT(*) as count
FROM `project.dataset.wiki_edits_ai_events`,
UNNEST(JSON_KEYS(ai_flags)) as flag_type
GROUP BY flag_type
ORDER BY count DESC;

-- Recent AI-detected events with details
SELECT
  event_id,
  detection_timestamp,
  user,
  title,
  domain,
  ai_flags,
  content_sample
FROM `project.dataset.wiki_edits_ai_events`
ORDER BY detection_timestamp DESC
LIMIT 50;

-- Events with specific AI pattern (e.g., ChatGPT artifacts)
SELECT
  event_id,
  user,
  title,
  domain,
  detection_timestamp,
  JSON_EXTRACT_SCALAR(ai_flags, '$.chatgpt_artifacts') as has_chatgpt_artifacts
FROM `project.dataset.wiki_edits_ai_events`
WHERE JSON_EXTRACT_SCALAR(ai_flags, '$.chatgpt_artifacts') = 'true'
ORDER BY detection_timestamp DESC;

-- ============================================================================
-- User Analysis
-- ============================================================================

-- Most active users
SELECT
  user,
  COUNT(*) as edit_count,
  SUM(CASE WHEN bot THEN 1 ELSE 0 END) as bot_edits,
  SUM(CASE WHEN minor THEN 1 ELSE 0 END) as minor_edits
FROM `project.dataset.wiki_edits`
GROUP BY user
ORDER BY edit_count DESC
LIMIT 100;

-- Users with AI-flagged content
SELECT
  user,
  COUNT(*) as ai_flagged_count,
  ARRAY_AGG(DISTINCT title LIMIT 10) as affected_pages
FROM `project.dataset.wiki_edits_ai_events`
GROUP BY user
HAVING ai_flagged_count > 1
ORDER BY ai_flagged_count DESC;

-- ============================================================================
-- Domain Analysis
-- ============================================================================

-- Events by Wikipedia domain
SELECT
  domain,
  COUNT(*) as event_count,
  SUM(CASE WHEN type = 'edit' THEN 1 ELSE 0 END) as edits,
  SUM(CASE WHEN type = 'new' THEN 1 ELSE 0 END) as new_pages
FROM `project.dataset.wiki_edits`
GROUP BY domain
ORDER BY event_count DESC;

-- AI detection rate by domain
SELECT
  main.domain,
  COUNT(DISTINCT main.evt_id) as total_events,
  COUNT(DISTINCT ai.event_id) as ai_events,
  ROUND(COUNT(DISTINCT ai.event_id) / COUNT(DISTINCT main.evt_id) * 100, 2) as ai_percentage
FROM `project.dataset.wiki_edits` main
LEFT JOIN `project.dataset.wiki_edits_ai_events` ai
  ON main.evt_id = ai.event_id
GROUP BY main.domain
HAVING total_events > 100
ORDER BY ai_percentage DESC;

-- ============================================================================
-- Temporal Analysis
-- ============================================================================

-- Events per hour
SELECT
  TIMESTAMP_TRUNC(TIMESTAMP(dt_user), HOUR) as hour,
  COUNT(*) as event_count
FROM `project.dataset.wiki_edits`
GROUP BY hour
ORDER BY hour DESC
LIMIT 24;

-- Events per day
SELECT
  DATE(dt_user) as day,
  COUNT(*) as event_count,
  COUNT(DISTINCT user) as unique_users
FROM `project.dataset.wiki_edits`
GROUP BY day
ORDER BY day DESC
LIMIT 30;

-- ============================================================================
-- Content Analysis
-- ============================================================================

-- Average edit size by namespace
SELECT
  ns as namespace,
  COUNT(*) as edit_count,
  AVG(len_new - len_old) as avg_size_change,
  MAX(len_new - len_old) as max_increase,
  MIN(len_new - len_old) as max_decrease
FROM `project.dataset.wiki_edits`
WHERE type = 'edit'
GROUP BY ns
ORDER BY edit_count DESC;

-- Large edits (>1000 bytes added)
SELECT
  evt_id,
  title,
  user,
  len_new - len_old as bytes_added,
  comment
FROM `project.dataset.wiki_edits`
WHERE type = 'edit'
  AND (len_new - len_old) > 1000
ORDER BY bytes_added DESC
LIMIT 100;

-- ============================================================================
-- Validation and Data Quality
-- ============================================================================

-- Check dead letter queue for errors
SELECT
  _error_category,
  COUNT(*) as error_count,
  ARRAY_AGG(DISTINCT _error_details LIMIT 5) as sample_errors
FROM `project.dataset.wiki_edits_dead_letter`
GROUP BY _error_category
ORDER BY error_count DESC;

-- Recent validation failures
SELECT
  _validation_timestamp,
  _error_category,
  _error_details,
  JSON_EXTRACT_SCALAR(_original_payload, '$.type') as event_type,
  JSON_EXTRACT_SCALAR(_original_payload, '$.title') as title
FROM `project.dataset.wiki_edits_dead_letter`
ORDER BY _validation_timestamp DESC
LIMIT 50;

-- ============================================================================
-- Performance Metrics
-- ============================================================================

-- Processing rate over time (events per minute)
SELECT
  TIMESTAMP_TRUNC(TIMESTAMP(dt_user), MINUTE) as minute,
  COUNT(*) as events_per_minute
FROM `project.dataset.wiki_edits`
WHERE DATE(dt_user) = CURRENT_DATE()
GROUP BY minute
ORDER BY minute DESC;

-- Data completeness check
SELECT
  COUNTIF(title IS NOT NULL) / COUNT(*) * 100 as title_completeness,
  COUNTIF(user IS NOT NULL) / COUNT(*) * 100 as user_completeness,
  COUNTIF(comment IS NOT NULL) / COUNT(*) * 100 as comment_completeness,
  COUNTIF(len_new IS NOT NULL) / COUNT(*) * 100 as length_completeness
FROM `project.dataset.wiki_edits`;

-- ============================================================================
-- Advanced Analytics
-- ============================================================================

-- AI detection patterns co-occurrence
WITH pattern_pairs AS (
  SELECT
    event_id,
    flag_type
  FROM `project.dataset.wiki_edits_ai_events`,
  UNNEST(JSON_KEYS(ai_flags)) as flag_type
  WHERE JSON_EXTRACT_SCALAR(ai_flags, CONCAT('$.', flag_type)) = 'true'
)
SELECT
  p1.flag_type as pattern_1,
  p2.flag_type as pattern_2,
  COUNT(*) as co_occurrence_count
FROM pattern_pairs p1
JOIN pattern_pairs p2
  ON p1.event_id = p2.event_id
  AND p1.flag_type < p2.flag_type  -- Avoid duplicates
GROUP BY pattern_1, pattern_2
HAVING co_occurrence_count > 5
ORDER BY co_occurrence_count DESC;

-- User edit patterns
SELECT
  user,
  COUNT(*) as total_edits,
  ROUND(AVG(len_new - len_old), 2) as avg_edit_size,
  COUNTIF(minor) / COUNT(*) * 100 as minor_edit_percentage,
  COUNT(DISTINCT DATE(dt_user)) as active_days
FROM `project.dataset.wiki_edits`
WHERE type = 'edit'
GROUP BY user
HAVING total_edits > 10
ORDER BY total_edits DESC
LIMIT 100;

-- Page edit frequency
SELECT
  title,
  COUNT(*) as edit_count,
  COUNT(DISTINCT user) as unique_editors,
  MIN(TIMESTAMP(dt_user)) as first_edit,
  MAX(TIMESTAMP(dt_user)) as last_edit
FROM `project.dataset.wiki_edits`
GROUP BY title
HAVING edit_count > 5
ORDER BY edit_count DESC
LIMIT 100;

-- ============================================================================
-- Export Queries
-- ============================================================================

-- Export AI events for analysis
-- (Run this and save results as CSV)
SELECT
  event_id,
  detection_timestamp,
  rule_version,
  user,
  title,
  domain,
  JSON_EXTRACT_SCALAR(ai_flags, '$.chatgpt_artifacts') as has_chatgpt,
  JSON_EXTRACT_SCALAR(ai_flags, '$.markdown_syntax') as has_markdown,
  JSON_EXTRACT_SCALAR(ai_flags, '$.emojis') as has_emojis,
  content_sample
FROM `project.dataset.wiki_edits_ai_events`
ORDER BY detection_timestamp DESC;

-- Export summary statistics
SELECT
  DATE(dt_user) as date,
  domain,
  COUNT(*) as total_events,
  COUNTIF(type = 'edit') as edits,
  COUNTIF(type = 'new') as new_pages,
  COUNTIF(bot) as bot_events,
  COUNT(DISTINCT user) as unique_users
FROM `project.dataset.wiki_edits`
GROUP BY date, domain
ORDER BY date DESC, domain;
