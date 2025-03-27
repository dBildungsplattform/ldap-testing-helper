#!/bin/bash

IMAGE_NAME="spsp_ldap_testing_helper"

# Load environment variables from env.list
docker build -t ${IMAGE_NAME} .

if [ $? -ne 0 ]; then
    echo "Docker build failed"
    exit 1
fi

docker run --network="host" \
            ${IMAGE_NAME}

