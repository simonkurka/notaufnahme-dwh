#!/bin/bash
set -e

PACKAGE=$1
VERSION=$2
DBUILD=$3

# Directory this script is located in + /resources
DRESOURCES="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )/resources"

mkdir -p $DBUILD/opt/wildfly/standalone/deployments/
wget -P $DBUILD/opt/wildfly/standalone/deployments https://www.aktin.org/software/repo/org/aktin/dwh/dwh-j2ee/$VERSION/dwh-j2ee-$VERSION.ear

mkdir -p $DBUILD/opt/wildfly/standalone/configuration/
mkdir -p $DBUILD/etc/aktin
cp $DRESOURCES/aktin.properties $DBUILD/etc/aktin/

#
# Post-install resources
#
mkdir -p $DBUILD/usr/share/$PACKAGE
cp -r $DRESOURCES/database $DRESOURCES/datasource $DBUILD/usr/share/$PACKAGE/

