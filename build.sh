#!/bin/bash
set -e

# Check if variables are empty
if [ -z "${PACKAGE}" ]; then echo "\$PACKAGE is empty."; exit 1; fi
if [ -z "${VERSION}" ]; then echo "\$VERSION is empty."; exit 1; fi
if [ -z "${DBUILD}" ]; then echo "\$DBUILD is empty."; exit 1; fi

# Directory this script is located in + /resources
DRESOURCES="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )/resources"

set -a
. "${DRESOURCES}/versions"
set +a

function dwh_j2ee() {
	DWILDFLYDEPLOYMENTS="${1}"

	mkdir -p "${DBUILD}${DWILDFLYDEPLOYMENTS}"
	mvn dependency:get -DremoteRepositories="https://www.aktin.org/software/repo/" -Dartifact="org.aktin.dwh:dwh-j2ee:${VDWH_J2EE}:ear"
	# dirty
	cp ~/".m2/repository/org/aktin/dwh/dwh-j2ee/${VDWH_J2EE}/dwh-j2ee-${VDWH_J2EE}.ear" "${DBUILD}${DWILDFLYDEPLOYMENTS}/dwh-j2ee-${VDWH_J2EE}.ear"
}

function aktin_properties() {
	DAKTINCONF="${1}"

	mkdir -p "${DBUILD}${DAKTINCONF}"
	cp "${DRESOURCES}/aktin.properties" "${DBUILD}${DAKTINCONF}/"
}

function aktin_dir() {
	DAKTIN="${1}"

	mkdir -p "${DBUILD}${DAKTIN}"
}

function aktin_importscripts() {
	DAKTINIMPORTSCRIPTS="${1}"

	mkdir -p "$(dirname "${DBUILD}${DAKTINIMPORTSCRIPTS}")"
	cp -r "${DRESOURCES}/import-scripts" "${DBUILD}${DAKTINIMPORTSCRIPTS}"
}

function aktin_importdir() {
	DAKTINIMPORT="${1}"

	mkdir -p "${DBUILD}${DAKTINIMPORT}"
}

function database_postinstall() {
	DDBPOSTINSTALL="$1"

	mkdir -p "$(dirname "${DBUILD}${DDBPOSTINSTALL}")"
	cp -r "${DRESOURCES}/database" "${DBUILD}${DDBPOSTINSTALL}"
}

function datasource_postinstall() {
	DDSPOSTINSTALL="$1"

	mkdir -p "$(dirname "${DBUILD}${DDSPOSTINSTALL}")"
	cp -r "${DRESOURCES}/datasource" "${DBUILD}${DDSPOSTINSTALL}"
}

function build_linux() {
	dwh_j2ee "/opt/wildfly/standalone/deployments"
	aktin_properties "/etc/aktin"
	aktin_dir "/var/lib/aktin"
	aktin_importscripts "/var/lib/aktin/import-scripts"
	aktin_importdir "/var/lib/aktin/import"
	database_postinstall "/usr/share/${PACKAGE}/database"
	datasource_postinstall "/usr/share/${PACKAGE}/datasource"
}

