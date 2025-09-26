#!/bin/bash
set -euo pipefail

REGION="${REGION:-us-east-1}"   # default if not set

# Extract repository names from docker-compose.yaml
for r in $(grep 'image: \${IMAGE_REGISTRY}' docker-compose.yaml \
    | sed -E 's|.*/([^:]+).*|\1|'); do
    
    echo "Checking if repository exists: $r"
    if aws ecr describe-repositories --repository-names "$r" --region "$REGION" >/dev/null 2>&1; then
        echo "Repository $r already exists, skipping..."
    else
        echo "Creating repository: $r"
        aws ecr create-repository --repository-name "$r" --region "$REGION"
    fi
done

echo "Building images..."
docker compose build

echo "Pushing images to ECR..."
docker compose push
