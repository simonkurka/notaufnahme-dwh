#!/bin/bash
set -e
VERSION=$1

# Directory this script is located in
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
DBUILD="$DIR/build/aktin-notaufnahme-dwh_$VERSION"

# Load common linux files
source $(dirname "$DIR")/build.sh "$DBUILD"

mkdir -p $DBUILD/DEBIAN
sed -e "s/__PACKAGE__/$PACKAGE/g" -e "s/__VERSION__/$VERSION/g" $DIR/control > $DBUILD/DEBIAN/control
cp $DIR/preinst $DBUILD/DEBIAN/
cp $DIR/postinst $DBUILD/DEBIAN/
cp $DIR/prerm $DBUILD/DEBIAN/
sed -e "/^__AKTIN_DROP__/{r $DRESOURCES/database/aktin_postgres_drop.sql" -e 'd;}' $DIR/postrm > $DBUILD/DEBIAN/postrm

#
# Changelog
#
function getChangelog() {
	function logentry() {
		local previous=$1
		local version=$2
		local urgency="low"
		local msg="$(git --no-pager log --format="%B" $previous${previous:+..}$version)"
		if [[ "$msg" =~ "urgency:"[[:space:]]*"critical" ]]; then urgency="critical"
		elif [[ "$msg" =~ "urgency:"[[:space:]]*"emergency" ]]; then urgency="emergency"
		elif [[ "$msg" =~ "urgency:"[[:space:]]*"high" ]]; then urgency="high"
		elif [[ "$msg" =~ "urgency:"[[:space:]]*"medium" ]]; then urgency="medium"
		fi
		echo "$PACKAGE ($version) stable; urgency=$urgency"
		echo
		git --no-pager log --format="  * %s" $previous${previous:+..}$version
		echo
		git --no-pager log --format=" -- %an <%ae>  %aD" -n 1 $version
		echo
	}
	git tag --sort "-version:refname" | grep "^v\?[0-9]\+\([.-][0-9]\+\)*" | (
		read version; while read previous; do
		logentry $previous $version
		version="$previous"
	done
	logentry "" $version
	)
}

getChangelog > $DBUILD/DEBIAN/changelog
editor $DBUILD/DEBIAN/changelog

dpkg-deb --build $DBUILD

