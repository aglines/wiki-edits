#!/bin/bash
# Timed Dataflow pipeline runner with drain monitoring
# Usage: ./runtimedpipeline.sh 2  (for 2 hours)

HOURS=${1:-2}  # Default 2 hours if not specified
SECONDS=$((HOURS * 3600))
REGION="us-central1"
JOB_PREFIX="wiki-edits"
MAX_DRAIN_WAIT=1800  # 30 minutes max wait for drain

echo "🚀 Starting Dataflow pipeline..."
python main.py pipeline

echo "⏱️  Will drain job after $HOURS hour(s)"
sleep $SECONDS

echo "🛑 Draining job..."
JOB_ID=$(gcloud dataflow jobs list \
  --region=$REGION \
  --status=active \
  --filter="name~$JOB_PREFIX" \
  --format="value(id)" \
  --limit=1)

if [ -z "$JOB_ID" ]; then
  echo "❌ No active job found"
  exit 1
fi

echo "📋 Job ID: $JOB_ID"
gcloud dataflow jobs drain $JOB_ID --region=$REGION

# Monitor drain status
echo "⏳ Monitoring drain progress (max wait: $((MAX_DRAIN_WAIT / 60)) minutes)..."
ELAPSED=0
CHECK_INTERVAL=30  # Check every 30 seconds

while [ $ELAPSED -lt $MAX_DRAIN_WAIT ]; do
  sleep $CHECK_INTERVAL
  ELAPSED=$((ELAPSED + CHECK_INTERVAL))
  
  STATUS=$(gcloud dataflow jobs describe $JOB_ID --region=$REGION --format="value(currentState)")
  
  case $STATUS in
    "JOB_STATE_DRAINED")
      echo "✅ Job successfully drained after $((ELAPSED / 60)) minutes"
      echo "🔔 NOTIFICATION: Dataflow job $JOB_ID is now DRAINED"
      exit 0
      ;;
    "JOB_STATE_CANCELLED")
      echo "✅ Job cancelled after $((ELAPSED / 60)) minutes"
      echo "🔔 NOTIFICATION: Dataflow job $JOB_ID is now CANCELLED"
      exit 0
      ;;
    "JOB_STATE_FAILED")
      echo "❌ Job failed during drain after $((ELAPSED / 60)) minutes"
      echo "🔔 NOTIFICATION: Dataflow job $JOB_ID FAILED"
      exit 1
      ;;
    "JOB_STATE_DRAINING")
      echo "⏳ Still draining... ($((ELAPSED / 60)) minutes elapsed)"
      ;;
    *)
      echo "⚠️  Unexpected status: $STATUS"
      ;;
  esac
done

# If we get here, drain is taking too long
echo "⚠️  WARNING: Drain exceeded $((MAX_DRAIN_WAIT / 60)) minutes"
echo "🔔 NOTIFICATION: Drain is taking too long. Forcing cancellation..."
gcloud dataflow jobs cancel $JOB_ID --region=$REGION

# Wait a bit and confirm cancellation
sleep 30
FINAL_STATUS=$(gcloud dataflow jobs describe $JOB_ID --region=$REGION --format="value(currentState)")
echo "🔔 NOTIFICATION: Job $JOB_ID final status: $FINAL_STATUS"
