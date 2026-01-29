#!/bin/bash
set -e

# Load configuration from .env file
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="${SCRIPT_DIR}/../.env"

if [ ! -f "$ENV_FILE" ]; then
    echo "❌ Error: .env file not found at $ENV_FILE"
    exit 1
fi

# Source .env file
set -a
source "$ENV_FILE"
set +a

# Configuration from .env
PROJECT_ID="${GCP_PROJECT_ID}"
REGION="${GCP_REGION}"
TEMPLATE_PATH="gs://${PROJECT_ID}-dataflow-templates/wiki-pipeline.json"

# Validate required variables
if [ -z "$PROJECT_ID" ] || [ -z "$REGION" ] || [ -z "$PUBSUB_TOPIC" ]; then
    echo "❌ Error: Missing required environment variables"
    echo "Required: GCP_PROJECT_ID, GCP_REGION, PUBSUB_TOPIC, PUBSUB_SUBSCRIPTION, GCS_TEMP_LOCATION, BIGQUERY_OUTPUT_TABLE"
    exit 1
fi

# Generate unique job name with timestamp
JOB_NAME="wiki-pipeline-$(date +%Y%m%d-%H%M%S)"

echo "🚀 Launching Dataflow job from template..."
echo "Job name: ${JOB_NAME}"
echo "Template: ${TEMPLATE_PATH}"

# Build custom parameters (only pipeline-specific, not infrastructure)
PARAMS="topic=${PUBSUB_TOPIC}"
PARAMS="${PARAMS},subscription=${PUBSUB_SUBSCRIPTION}"
PARAMS="${PARAMS},output_table=${BIGQUERY_OUTPUT_TABLE}"

# Build Dataflow-native flags (not custom parameters)
DATAFLOW_FLAGS=""
if [ -n "${DATAFLOW_MAX_WORKERS}" ]; then
  DATAFLOW_FLAGS="${DATAFLOW_FLAGS} --max-workers=${DATAFLOW_MAX_WORKERS}"
fi
if [ -n "${DATAFLOW_NUM_WORKERS}" ]; then
  DATAFLOW_FLAGS="${DATAFLOW_FLAGS} --num-workers=${DATAFLOW_NUM_WORKERS}"
fi
if [ -n "${DATAFLOW_MACHINE_TYPE}" ]; then
  DATAFLOW_FLAGS="${DATAFLOW_FLAGS} --worker-machine-type=${DATAFLOW_MACHINE_TYPE}"
fi
if [ -n "${DATAFLOW_DISK_SIZE_GB}" ]; then
  DATAFLOW_FLAGS="${DATAFLOW_FLAGS} --disk-size-gb=${DATAFLOW_DISK_SIZE_GB}"
fi
if [ "${DATAFLOW_STREAMING_ENGINE}" = "true" ]; then
  DATAFLOW_FLAGS="${DATAFLOW_FLAGS} --enable-streaming-engine"
fi

# Launch job from Flex Template
gcloud dataflow flex-template run ${JOB_NAME} \
  --template-file-gcs-location=${TEMPLATE_PATH} \
  --region=${REGION} \
  --project=${PROJECT_ID} \
  --parameters ${PARAMS} \
  ${DATAFLOW_FLAGS}

echo "✅ Job launched successfully!"
echo "Monitor at: https://console.cloud.google.com/dataflow/jobs/${REGION}/${JOB_NAME}?project=${PROJECT_ID}"
