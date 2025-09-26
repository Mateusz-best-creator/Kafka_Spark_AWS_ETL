#!/bin/bash

# docker login --username AWS --password-stdin ${IMAGE_REGISTRY}.dkr.ecr.${REGION}.amazonaws.com

docker-compose pull
docker-compose up