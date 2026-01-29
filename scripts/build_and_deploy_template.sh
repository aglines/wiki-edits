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
TEMPLATE_IMAGE="gcr.io/${PROJECT_ID}/wiki-pipeline:latest"
TEMPLATE_PATH="gs://${PROJECT_ID}-dataflow-templates/wiki-pipeline.json"

echo "🔨 Building and deploying Dataflow Flex Template..."
echo "Project: ${PROJECT_ID}"
echo "Image: ${TEMPLATE_IMAGE}"
echo "Template: ${TEMPLATE_PATH}"

# Build and push container image using Cloud Build
echo "📦 Building container image..."
gcloud builds submit --tag ${TEMPLATE_IMAGE} --project ${PROJECT_ID} .

# Create Flex Template
echo "📝 Creating Flex Template..."
gcloud dataflow flex-template build ${TEMPLATE_PATH} \
  --image ${TEMPLATE_IMAGE} \
  --sdk-language PYTHON \
  --metadata-file template_metadata.json \
  --project ${PROJECT_ID}

echo "✅ Template deployed successfully!"
echo "Template location: ${TEMPLATE_PATH}"
