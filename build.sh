#!/bin/bash
set -e

PACKAGE=aktin-notaufnahme-dwh

DBUILD=$1
VERSION=$2

# Directory this script is located in + /resources
DRESOURCES="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )/resources"

mkdir -p $DBUILD/opt/wildfly/standalone/deployments/
wget -P $DBUILD/opt/wildfly/standalone/deployments https://github.com/aktin/dwh-j2ee/releases/download/v$VERSION/dwh-j2ee-$VERSION.ear
cp $DRESOURCES/datasource/* $DBUILD/opt/wildfly/standalone/deployments/


mkdir -p $DBUILD/opt/wildfly/standalone/configuration/
cp $DRESOURCES/aktin.properties $DBUILD/opt/wildfly/standalone/configuration/

#
# Post-install resources
#
mkdir -p $DBUILD/usr/share/$PACKAGE
cp -r $DRESOURCES/database $DBUILD/usr/share/$PACKAGE/

