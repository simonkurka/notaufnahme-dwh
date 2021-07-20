#!/bin/bash
set -e

# Required parameters
PACKAGE="${1}"
VERSION="${2}"

# Optional parameter
FULL="${3}"

# Check if variables are empty
if [ -z "${PACKAGE}" ]; then echo "\$PACKAGE is empty."; exit 1; fi
if [ -z "${VERSION}" ]; then echo "\$VERSION is empty."; exit 1; fi

# Directory this script is located in
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
DBUILD="${DIR}/build"

# Cleanup
rm -rf "${DIR}/build"

export I2B2IMAGENAMESPACE="$(echo "${PACKAGE}" | awk -F '-' '{print "ghcr.io/"$1"/"$2"-i2b2-"}')"
export DWHIMAGENAMESPACE="$(echo "${PACKAGE}" | awk -F '-' '{print "ghcr.io/"$1"/"$2"-dwh-"}')"

# Load common linux files
. "$(dirname "${DIR}")/build.sh"

mkdir -p "${DBUILD}/wildfly"
sed -e "s|__BASEIMAGE__|${I2B2IMAGENAMESPACE}wildfly|g" wildfly/Dockerfile >"${DBUILD}/wildfly/Dockerfile"
cp "${DRESOURCES}/aktin.properties" "${DBUILD}/wildfly/"
dwh_j2ee "/wildfly"
aktin_properties "/wildfly"
aktin_importscripts "/wildfly"
datasource_postinstall "/wildfly/ds"

mkdir -p "${DBUILD}/database"
sed -e "s|__BASEIMAGE__|${I2B2IMAGENAMESPACE}database|g" database/Dockerfile >"${DBUILD}/database/Dockerfile"
database_postinstall "/database/sql"

mkdir -p "${DBUILD}/httpd"
sed -e "s|__BASEIMAGE__|${I2B2IMAGENAMESPACE}httpd|g" httpd/Dockerfile >"${DBUILD}/httpd/Dockerfile"

if [ "${FULL}" = "full" ]; then
	docker-compose build
fi

