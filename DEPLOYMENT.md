# Deployment Guide

This guide covers how to deploy and run the `cmbdemo` tool.

## Prerequisites

- Python 3.8+
- Google Cloud SDK (`gcloud`) installed and configured.
- A Google Cloud Project with billing enabled.
- APIs enabled:
    - Dataplex API
    - BigQuery API
    - Cloud Storage API
    - Cloud Run API (if deploying to Cloud Run)
    - Artifact Registry API (if deploying to Cloud Run)

## Local Installation

You can install the package locally in editable mode:

```bash
pip install -e .
```

Run the tool:

```bash
python3 -m cmbdemo.main --config config.yaml
```

## Packaging

To build a distribution package (wheel and sdist):

```bash
pip install build
python3 -m build
```

The artifacts will be created in the `dist/` directory.

## Cloud Deployment (Cloud Run Job)

For scheduled or remote execution, you can deploy the tool as a Cloud Run Job.

### 1. Create a Dockerfile

Create a `Dockerfile` in the root of the project:

```dockerfile
FROM python:3.11-slim

WORKDIR /app

COPY . .

RUN pip install .

ENTRYPOINT ["python3", "-m", "cmbdemo.main"]
```

### 2. Build and Push Image

```bash
export PROJECT_ID="your-project-id"
export REGION="us-central1"
export REPO_NAME="cmbdemo-repo"
export IMAGE_NAME="cmbdemo-tool"

# Create Artifact Registry repository (if not exists)
gcloud artifacts repositories create $REPO_NAME \
    --repository-format=docker \
    --location=$REGION \
    --description="Repository for cmbdemo"

# Build and submit
gcloud builds submit --tag $REGION-docker.pkg.dev/$PROJECT_ID/$REPO_NAME/$IMAGE_NAME .
```

### 3. Create Cloud Run Job

You need to mount the `config.yaml` or pass configuration via environment variables. For simplicity, let's assume we bake the config into the image or mount it as a secret.

If you want to pass the config file content as a secret:

1.  Create a secret in Secret Manager with the content of `config.yaml`.
2.  Mount it in the job.

Alternatively, for a simple test, you can `COPY config.yaml .` in the Dockerfile (not recommended for production secrets).

**Deploy command:**

```bash
gcloud run jobs create cmbdemo-job \
    --image $REGION-docker.pkg.dev/$PROJECT_ID/$REPO_NAME/$IMAGE_NAME \
    --region $REGION \
    --args="--config,config.yaml" \
    --set-secrets="/app/config.yaml=YOUR_SECRET_NAME:latest"
```

### 4. Execute the Job

```bash
gcloud run jobs execute cmbdemo-job --region $REGION
```

## Scheduling

You can schedule the Cloud Run Job using Cloud Scheduler:

```bash
gcloud scheduler jobs create http cmbdemo-schedule \
    --schedule="0 2 * * *" \
    --uri="https://$REGION-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/$PROJECT_ID/jobs/cmbdemo-job:run" \
    --http-method=POST \
    --oauth-service-account-email="YOUR_SERVICE_ACCOUNT_EMAIL"
```
