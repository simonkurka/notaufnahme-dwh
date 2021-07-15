#!/bin/bash
set -e

PACKAGE=$1
VERSION=$2
DBUILD=$3

# Directory this script is located in + /resources
DRESOURCES="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )/resources"

mkdir -p $DBUILD/opt/wildfly/standalone/deployments/
wget -P $DBUILD/opt/wildfly/standalone/deployments https://www.aktin.org/software/repo/org/aktin/dwh/dwh-j2ee/$VERSION/dwh-j2ee-$VERSION.ear
cp $DRESOURCES/datasource/* $DBUILD/opt/wildfly/standalone/deployments/


mkdir -p $DBUILD/opt/wildfly/standalone/configuration/
cp $DRESOURCES/aktin.properties $DBUILD/opt/wildfly/standalone/configuration/

#
# Post-install resources
#
mkdir -p $DBUILD/usr/share/$PACKAGE
cp -r $DRESOURCES/database $DBUILD/usr/share/$PACKAGE/

