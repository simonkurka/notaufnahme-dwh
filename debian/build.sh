#!/bin/bash
set -e
PACKAGE=$1
VERSION=$2

# Directory this script is located in
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
DBUILD="$DIR/build/${PACKAGE}_${VERSION}"

# Load common linux files
source $(dirname "$DIR")/build.sh "$PACKAGE" "$VERSION" "$DBUILD"

mkdir -p $DBUILD/DEBIAN
sed -e "s/__PACKAGE__/$PACKAGE/g" -e "s/__VERSION__/$VERSION/g" $DIR/control > $DBUILD/DEBIAN/control
cp $DIR/preinst $DBUILD/DEBIAN/
cp $DIR/postinst $DBUILD/DEBIAN/
cp $DIR/prerm $DBUILD/DEBIAN/
sed -e "/^__AKTIN_DROP__/{r $DRESOURCES/database/aktin_postgres_drop.sql" -e 'd;}' $DIR/postrm > $DBUILD/DEBIAN/postrm && chmod 0755 $DBUILD/DEBIAN/postrm

dpkg-deb --build $DBUILD

