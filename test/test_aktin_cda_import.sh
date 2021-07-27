#! /bin/bash

set -euo pipefail

readonly INTEGRATION_ROOT=$(pwd)/resources
readonly INTEGRATION_BINARIES=$INTEGRATION_ROOT/binaries
readonly XML_FILES=$INTEGRATION_ROOT/xml
readonly PROPERTIES_FILES=$INTEGRATION_ROOT/properties

# colors for console output
readonly WHI=${color_white}
readonly RED=${color_red}
readonly ORA=${color_orange}
readonly YEL=${color_yellow}
readonly GRE=${color_green}

# destination url for fhir testing
URL="http://localhost:80/aktin/cda/fhir/Binary/"

# set timestamp to compare saving of sent CDA files
CURRENT_HOUR=$(date "+%Y-%m-%d %H")

# loop over all storyboards
STORYBOARD=( aktin_test_storyboard01.xml aktin_test_storyboard01.xml aktin_test_storyboard01_error.xml aktin_test_storyboard02.xml aktin_test_storyboard03.xml )
CODE=( 201 200 422 201 201 )
for i in "${!STORYBOARD[@]}"
do
	# sent CDA document via java-demo-server-fhir-client and catch response
	RESPONSE=$(java -Djava.util.logging.config.file="$PROPERTIES_FILES/logging.properties" -cp "$INTEGRATION_BINARIES/demo-server-${org.aktin.demo-distribution.version}.jar" org.aktin.cda.etl.demo.client.FhirClient $URL $XML_FILES/${STORYBOARD[$i]} 2<&1)

	# extract response code and compare with predefined code, print whole response on failure
	RESPONSE_CODE=$(echo $RESPONSE | grep -oP '(?<=Response code: )[0-9]+')
	if [[ $RESPONSE_CODE == ${CODE[$i]} ]]; then
		echo -e "${GRE}${STORYBOARD[$i]} successful ($RESPONSE_CODE)${WHI}"
	else
		echo -e "${RED}${STORYBOARD[$i]} failed ($RESPONSE_CODE)${WHI}"
		echo $RESPONSE
		exit 1
	fi
done

# count entries in i2b2crcdata with import_date within previously set timestamp
if [[ $(sudo -u postgres psql -d i2b2 -v ON_ERROR_STOP=1 -c "SELECT import_date FROM i2b2crcdata.encounter_mapping" | grep -c "$CURRENT_HOUR") != 0 ]]; then
	echo -e "${GRE} --> new entries in i2b2 detected${WHI}"
else
	echo -e "${RED} --> no new entries in i2b2 detected${WHI}"
fi
