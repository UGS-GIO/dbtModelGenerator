#!/bin/bash

# Deploy dbt-model-generator Cloud Function
# This script:
# 1. Creates the Pub/Sub topic
# 2. Creates the GCS bucket for dbt models
# 3. Deploys the Cloud Function
# 4. Sets up IAM permissions

set -e

PROJECT_ID="ut-dnr-ugs-backend-tools"
REGION="us-central1"
FUNCTION_NAME="dbt-model-generator"
TOPIC_NAME="dbt-model-generation"
BUCKET_NAME="ugs_dbt_models"
SERVICE_ACCOUNT="gdb-ingest-processor@${PROJECT_ID}.iam.gserviceaccount.com"

echo "============================================"
echo "Deploying dbt Model Generator"
echo "============================================"

# 1. Create Pub/Sub topic if it doesn't exist
echo "📬 Creating Pub/Sub topic: ${TOPIC_NAME}..."
gcloud pubsub topics create ${TOPIC_NAME} \
  --project=${PROJECT_ID} \
  2>/dev/null || echo "Topic already exists"

# 2. Create GCS bucket for dbt models if it doesn't exist
echo "📦 Creating GCS bucket: ${BUCKET_NAME}..."
gcloud storage buckets create gs://${BUCKET_NAME} \
  --project=${PROJECT_ID} \
  --location=${REGION} \
  --uniform-bucket-level-access \
  2>/dev/null || echo "Bucket already exists"

# 3. Set up bucket structure
echo "📁 Creating bucket folder structure..."
echo "" | gsutil cp - gs://${BUCKET_NAME}/models/staging/.keep
echo "" | gsutil cp - gs://${BUCKET_NAME}/models/intermediate/.keep
echo "" | gsutil cp - gs://${BUCKET_NAME}/models/marts/.keep

# 4. Deploy Cloud Function
echo "🚀 Deploying Cloud Function: ${FUNCTION_NAME}..."
gcloud functions deploy ${FUNCTION_NAME} \
  --gen2 \
  --runtime=nodejs20 \
  --region=${REGION} \
  --source=. \
  --entry-point=generateDbtModels \
  --trigger-topic=${TOPIC_NAME} \
  --timeout=540s \
  --memory=512MB \
  --service-account=${SERVICE_ACCOUNT} \
  --set-env-vars="GCP_PROJECT=${PROJECT_ID}" \
  --project=${PROJECT_ID}



# Allow function to write to GCS bucket
gcloud storage buckets add-iam-policy-binding gs://${BUCKET_NAME} \
  --member="serviceAccount:${SERVICE_ACCOUNT}" \
  --role="roles/storage.objectAdmin" \
  --project=${PROJECT_ID}

# Allow gdb-ingest-processor to publish to topic
gcloud pubsub topics add-iam-policy-binding ${TOPIC_NAME} \
  --member="serviceAccount:${SERVICE_ACCOUNT}" \
  --role="roles/pubsub.publisher" \
  --project=${PROJECT_ID}

echo ""
echo "============================================"
echo "✅ Deployment Complete!"
echo "============================================"
echo ""
echo "📊 Function Details:"
echo "   Name: ${FUNCTION_NAME}"
echo "   Region: ${REGION}"
echo "   Trigger: Pub/Sub topic '${TOPIC_NAME}'"
echo "   Output: gs://${BUCKET_NAME}/"
echo ""
echo "🧪 Test with:"
echo "   gcloud pubsub topics publish ${TOPIC_NAME} \\"
echo "     --message='{\"baseTableName\":\"geolunits_alluvial_fan\",\"schemaVersion\":\"v1\",\"operation\":\"new_table\"}' \\"
echo "     --project=${PROJECT_ID}"
echo ""
echo "📝 View logs:"
echo "   gcloud functions logs read ${FUNCTION_NAME} --region=${REGION} --project=${PROJECT_ID}"
echo ""