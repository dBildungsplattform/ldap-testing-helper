#!/bin/bash

IMAGE_NAME="spsp_migration"

# Load environment variables from env.list
set -a
source env.list
set +a

# Check if the migration type is 'SCHOOLS'
if [ "$MIGRATION_TYPE" == "SCHOOLS" ]; then
    docker build -t ${IMAGE_NAME} .

    if [ $? -ne 0 ]; then
        echo "Docker build failed"
        exit 1
    fi

    ABS_INPUT_PATH_EXCEL=$(realpath "$MIGRATION_SCHOOLS_INPUT_EXCEL")
    ABS_INPUT_PATH_LDAP=$(realpath "$MIGRATION_SCHOOLS_INPUT_LDAP")

    INPUT_DIR_EXCEL=$(dirname "${ABS_INPUT_PATH_EXCEL}")
    INPUT_FILE_EXCEL=$(basename "${ABS_INPUT_PATH_EXCEL}")

    INPUT_DIR_LDAP=$(dirname "${ABS_INPUT_PATH_LDAP}")
    INPUT_FILE_LDAP=$(basename "${ABS_INPUT_PATH_LDAP}")

    docker run --network="host" \
               --env-file env.list \
               -v "${INPUT_DIR_EXCEL}:/usr/src/app/data" \
               -v "${INPUT_DIR_LDAP}:/usr/src/app/data" \
               -e MIGRATION_SCHOOLS_INPUT_EXCEL_COMPLETE_PATH="/usr/src/app/data/${INPUT_FILE_EXCEL}" \
               -e MIGRATION_SCHOOLS_INPUT_LDAP_COMPLETE_PATH="/usr/src/app/data/${INPUT_FILE_LDAP}" \
               ${IMAGE_NAME}
               
elif [ "$MIGRATION_TYPE" == "PERSONS" ]; then
    docker build -t ${IMAGE_NAME} .

    if [ $? -ne 0 ]; then
        echo "Docker build failed"
        exit 1
    fi

    ABS_INPUT_PATH_LDAP=$(realpath "$MIGRATION_PERSONS_INPUT_LDAP")
    INPUT_DIR_LDAP=$(dirname "${ABS_INPUT_PATH_LDAP}")
    INPUT_FILE_LDAP=$(basename "${ABS_INPUT_PATH_LDAP}")

    docker run --network="host" \
               --env-file env.list \
               -v "${INPUT_DIR_LDAP}:/usr/src/app/data" \
               -v "${PWD}/output:/usr/src/app/output" \
               -e MIGRATION_PERSONS_INPUT_LDAP_COMPLETE_PATH="/usr/src/app/data/${INPUT_FILE_LDAP}" \
               ${IMAGE_NAME}
else
    echo "Invalid migration type or operation aborted. No action taken."
fi
