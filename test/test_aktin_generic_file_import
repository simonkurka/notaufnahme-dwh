#! /bin/bash

set -euo pipefail

readonly INTEGRATION_ROOT=$(pwd)/resources
readonly INTEGRATION_BINARIES=$INTEGRATION_ROOT/binaries
readonly INTEGRATION_PROPERTIES=$INTEGRATION_ROOT/properties
readonly INTEGRATION_XML=$INTEGRATION_ROOT/xml

# colors for console output
readonly WHI=${color_white}
readonly RED=${color_red}
readonly ORA=${color_orange}
readonly YEL=${color_yellow}
readonly GRE=${color_green}

# destination url
URL='http://localhost:80/aktin/admin/rest'
URL_FHIR='http://localhost:80/aktin/cda/fhir/Binary/'

# login into aktin/admin and get bearer-token
BEARER_TOKEN=$(curl -s --location --request POST $URL/auth/login/ --header 'Content-Type: application/json' --data-raw '{ "username": "i2b2", "password": "demouser" }')

# # # # # # # # #
# GETTER for import scripts (should be 5 scripts)
# # # # # # # # #
COUNT_SCRIPTS=$(echo $(curl -s --location --request GET $URL/script | grep -o 'id' | wc -l))
if [[ $COUNT_SCRIPTS == 5 ]]; then
    echo -e "${GRE}GET scripts successful (GOT $COUNT_SCRIPTS)${WHI}"
else
    echo -e "${RED}GET scripts failed (GOT $COUNT_SCRIPTS)${WHI}"
    echo $(curl -s --request GET $URL/script)
    exit 1
fi

# # # # # # # # #
# upload p21.zip with all possible scripts
# # # # # # # # #
ID_SCRIPTS=( 'error' 'exit' 'sleep' 'success' 'p21import' )
for i in "${!ID_SCRIPTS[@]}"
do
    RESPONSE_CODE=$(curl -s -o /dev/null -w '%{http_code}' -F "data=@$INTEGRATION_BINARIES/p21.zip" --location --request POST ''$URL'/file?scriptId='${ID_SCRIPTS[$i]}'&filename=FILE_'$i'' --header 'Authorization: Bearer '$BEARER_TOKEN'')
    if [[ $RESPONSE_CODE == 201 ]]; then
	    echo -e "${GRE}UPLOAD of FILE_$i successful ($RESPONSE_CODE)${WHI}"
    else
	    echo -e "${RED}UPLOAD of FILE_$i failed ($RESPONSE_CODE)${WHI}"
	    exit 1
    fi
done

# # # # # # # # #
# GETTER for uploaded files (should be 5 files)
# # # # # # # # #
COUNT_FILES=$(echo $(curl -s --location --request GET $URL/file | grep -o 'id' | wc -l))
if [[ $COUNT_FILES == 5 ]]; then
    echo -e "${GRE}GET file successful (GOT $COUNT_FILES)${WHI}"
else
    echo -e "${RED}GET file failed (GOT $COUNT_FILES)${WHI}"
    echo $(curl -s --location --request GET $URL/file)
    exit 1
fi

# # # # # # # # #
# get uuid of uploaded files and match to corresponding script id
# # # # # # # # #
for i in "${!ID_SCRIPTS[@]}"
do
    declare ID_${ID_SCRIPTS[$i]^^}=$(curl -s --request GET $URL/file | jq '.[] | select(.script=="'${ID_SCRIPTS[$i]}'") | .id' | cut -d "\"" -f 2)
done
UUID_SCRIPTS=( $ID_ERROR $ID_EXIT $ID_SLEEP $ID_SUCCESS $ID_P21IMPORT )

# # # # # # # # #
# start file verification of each file
# # # # # # # # #
for i in "${!ID_SCRIPTS[@]}"
do
    RESPONSE_CODE=$(curl -s -o /dev/null -w '%{http_code}' --location --request POST $URL/script/${UUID_SCRIPTS[$i]}/verify --header 'Authorization: Bearer '$BEARER_TOKEN'')
    if [[ $RESPONSE_CODE == 204 ]]; then
	    echo -e "${GRE}VERIFY of FILE_$i successful ($RESPONSE_CODE)${WHI}"
    else
	    echo -e "${RED}VERIFY of FILE_$i failed ($RESPONSE_CODE)${WHI}"
	    exit 1
    fi
done

# # # # # # # # #
# wait till all scripts are finished
# # # # # # # # #
echo "wait 30s"
sleep 30s

# # # # # # # # #
# check script processing and state
# # # # # # # # #
for i in "${!ID_SCRIPTS[@]}"
do
    OPERATION=$(grep '^operation=' /var/lib/aktin/import/${UUID_SCRIPTS[$i]}/properties | cut -d'=' -f2)
    if [[ $OPERATION == 'verifying' ]]; then
        echo -e "${GRE}${UUID_SCRIPTS[$i]} successfully changed operation to verifying${WHI}"
    else
        echo -e "${RED}${UUID_SCRIPTS[$i]} has operation $OPERATION (should be verifying)${WHI}"
	    exit 1
    fi

    STATE=$(grep '^state=' /var/lib/aktin/import/${UUID_SCRIPTS[$i]}/properties | cut -d'=' -f2)
    case "${ID_SCRIPTS[$i]}" in
    'error' | 'exit')
        if [[ $STATE == 'failed' ]]; then
            echo -e "${GRE}${UUID_SCRIPTS[$i]} successfully changed state to failed${WHI}"
        else
            echo -e "${RED}${UUID_SCRIPTS[$i]} has state $STATE (should be failed)${WHI}"
	        exit 1
        fi
        ;;
    'sleep')
        if [[ $STATE == 'timeout' ]]; then
            echo -e "${GRE}${UUID_SCRIPTS[$i]} successfully changed state to timeout${WHI}"
        else
            echo -e "${RED}${UUID_SCRIPTS[$i]} has state $STATE (should be timeout)${WHI}"
	        exit 1
        fi
        ;;
    'success' | 'p21import')
        if [[ $STATE == 'successful' ]]; then
            echo -e "${GRE}${UUID_SCRIPTS[$i]} successfully changed state to successful${WHI}"
        else
            echo -e "${RED}${UUID_SCRIPTS[$i]} has state $STATE (should be successful)${WHI}"
	        exit 1
        fi
        ;;
    esac
done

# # # # # # # # #
# check created logs during script processing via endpoint
# # # # # # # # #
PATH_LOG=/var/lib/aktin/import
for i in "${!ID_SCRIPTS[@]}"
do
    if [[ -f $PATH_LOG/${UUID_SCRIPTS[$i]}/stdOutput && -f $PATH_LOG/${UUID_SCRIPTS[$i]}/stdError ]]; then
        echo -e "${GRE}Script logs for ${UUID_SCRIPTS[$i]} found${WHI}"
    else
        echo -e "${RED}No script logs for ${UUID_SCRIPTS[$i]} found${WHI}"
    fi

    LOG_ERROR=$(curl -s --request GET $URL/file/${UUID_SCRIPTS[$i]}/log | jq '.[] | select(.type=="stdError") | .text' | cut -d "\"" -f 2)
    LOG_OUTPUT=$(curl -s --request GET $URL/file/${UUID_SCRIPTS[$i]}/log | jq '.[] | select(.type=="stdOutput") | .text' | cut -d "\"" -f 2)

    case "${ID_SCRIPTS[$i]}" in
    'error' | 'exit')
        if [[ ! -z $LOG_ERROR && -z $LOG_OUTPUT ]]; then
            echo -e "${GRE}${UUID_SCRIPTS[$i]} has an error log and an empty output log${WHI}"
        else
            echo -e "${RED}Something is wrong with the logs of ${UUID_SCRIPTS[$i]}${WHI}"
	        exit 1
        fi
        ;;
    'sleep')
        if [[ -z $LOG_ERROR && -z $LOG_OUTPUT ]]; then
            echo -e "${GRE}${UUID_SCRIPTS[$i]} has empty logs${WHI}"
        else
            echo -e "${RED}Something is wrong with the logs of ${UUID_SCRIPTS[$i]}${WHI}"
	        exit 1
        fi
        ;;
    'success' | 'p21import')
        if [[ -z $LOG_ERROR && ! -z $LOG_OUTPUT ]]; then
            echo -e "${GRE}${UUID_SCRIPTS[$i]} has an output log and an empty error log${WHI}"
        else
            echo -e "${RED}Something is wrong with the logs of ${UUID_SCRIPTS[$i]}${WHI}"
	        exit 1
        fi
        ;;
    esac
done

# # # # # # # # #
# check cancelling of file processing
# # # # # # # # #
if [[ $(curl -s --request GET $URL/script/queue) == 0 ]]; then
    echo -e "${GRE}Queue is currently empty${WHI}"
else
    echo -e "${ORA}Queue is not empty${WHI}"
fi

RESPONSE_CODE=$(curl -s -o /dev/null -w '%{http_code}' --location --request POST $URL/script/$ID_SLEEP/verify --header 'Authorization: Bearer '$BEARER_TOKEN'')
if [[ $RESPONSE_CODE == 204 ]]; then
    echo -e "${GRE}VERIFY of FILE_$i successful ($RESPONSE_CODE)${WHI}"
    sleep 2s
else
    echo -e "${RED}VERIFY of FILE_$i failed ($RESPONSE_CODE)${WHI}"
    exit 1
fi

OPERATION=$(grep '^operation=' /var/lib/aktin/import/$ID_SLEEP/properties | cut -d'=' -f2)
if [[ $OPERATION == 'verifying' ]]; then
    echo -e "${GRE}$ID_SLEEP successfully changed operation to verifying${WHI}"
else
    echo -e "${ORA}$ID_SLEEP has operation $OPERATION (should be verifying)${WHI}"
fi

STATE=$(grep '^state=' /var/lib/aktin/import/$ID_SLEEP/properties | cut -d'=' -f2)
if [[ $STATE == 'in_progress' ]]; then
    echo -e "${GRE}$ID_SLEEP successfully changed state to in_progress${WHI}"
else
    echo -e "${ORA}$ID_SLEEP has state $STATE (should be in_progress)${WHI}"
fi

RESPONSE_CODE=$(curl -s -o /dev/null -w '%{http_code}' --location --request POST $URL/script/$ID_SLEEP/cancel --header 'Authorization: Bearer '$BEARER_TOKEN'')
if [[ $RESPONSE_CODE == 204 ]]; then
    echo -e "${GRE}CANCEL of FILE_$i successful ($RESPONSE_CODE)${WHI}"
else
    echo -e "${RED}CANCEL of FILE_$i failed ($RESPONSE_CODE)${WHI}"
    exit 1
fi

STATE=$(grep '^state=' /var/lib/aktin/import/$ID_SLEEP/properties | cut -d'=' -f2)
if [[ $STATE == 'cancelled' ]]; then
    echo -e "${GRE}$ID_SLEEP successfully changed state to cancelled${WHI}"
else
    echo -e "${ORA}$ID_SLEEP has state $STATE (should be cancelled)${WHI}"
fi

if [[ $(curl -s --request GET $URL/script/queue) == 0 ]]; then
    echo -e "${GRE}Queue is currently empty${WHI}"
else
    echo -e "${RED}Queue is not empty${WHI}"
    exit 1
fi

# # # # # # # # #
# upload patients (necessary to test p21 import)
# # # # # # # # #
CURRENT_DAY=$(date '+%Y-%m-%d')
for x in {1000..1138};
do
    sed -i "s|<id root=\"1.2.276.0.76.4.8\" extension=.*|<id root=\"1.2.276.0.76.4.8\" extension=\"P$x\"/>|g" $INTEGRATION_XML/aktin_test_storyboard06.xml
    sed -i "s|<id root=\"1.2.276.0.76.3.87686\" extension=.*|<id root=\"1.2.276.0.76.3.87686\" extension=\"$x\"/>|g" $INTEGRATION_XML/aktin_test_storyboard06.xml

	RESPONSE=$(java -Djava.util.logging.config.file="$INTEGRATION_PROPERTIES/logging.properties" -cp "$INTEGRATION_BINARIES/demo-server-0.14.jar" org.aktin.cda.etl.demo.client.FhirClient $URL_FHIR $INTEGRATION_XML/aktin_test_storyboard06.xml 2<&1)

	RESPONSE_CODE=$(echo $RESPONSE | grep -oP '(?<=Response code: )[0-9]+')
    case "$RESPONSE_CODE" in
    200)
        echo -e "${GRE}Updated patient P$x${WHI}"
        ;;
    201)
        echo -e "${GRE}Imported patient P$x${WHI}"
        ;;
    *)
		echo -e "${RED}Upload of patient P$x failed ($RESPONSE_CODE)${WHI}"
		echo $RESPONSE
		exit 1
        ;;
    esac
done

# # # # # # # # #
# check uploaded patients in the current day (should be not zero)
# # # # # # # # #
if [[ $(sudo -u postgres psql -d i2b2 -v ON_ERROR_STOP=1 -c "SELECT import_date FROM i2b2crcdata.encounter_mapping" | grep -c "$CURRENT_DAY") != 0 ]]; then
	echo -e "${GRE} --> new entries in i2b2 detected${WHI}"
else
	echo -e "${RED} --> no new entries in i2b2 detected${WHI}"
fi

# # # # # # # # #
# check imported p21 data (should be zero)
# # # # # # # # #
COUNT_P21=$(sudo -u postgres psql -X -A -d i2b2 -v ON_ERROR_STOP=1 -t -c "SELECT COUNT(provider_id) FROM i2b2crcdata.observation_fact WHERE provider_id='P21'")
if [[ $COUNT_P21 == 0 ]]; then
    echo -e "${GRE}No imported P21 data found${WHI}"
else
    echo -e "${RED}P21 are already imported (found $COUNT_P21)${WHI}"
    exit 1
fi

# # # # # # # # #
# start file import of p21.zip
# # # # # # # # #
RESPONSE_CODE=$(curl -s -o /dev/null -w '%{http_code}' --location --request POST $URL/script/$ID_P21IMPORT/import --header 'Authorization: Bearer '$BEARER_TOKEN'')
if [[ $RESPONSE_CODE == 204 ]]; then
    echo -e "${GRE}IMPORT of FILE_$i successful ($RESPONSE_CODE)${WHI}"
else
    echo -e "${RED}IMPORT of FILE_$i failed ($RESPONSE_CODE)${WHI}"
    exit 1
fi

echo "wait 20s"
sleep 20s

# # # # # # # # #
# check if file import of p21.zip was successul
# # # # # # # # #
OPERATION=$(grep '^operation=' /var/lib/aktin/import/$ID_P21IMPORT/properties | cut -d'=' -f2)
if [[ $OPERATION == 'importing' ]]; then
    echo -e "${GRE}$ID_P21IMPORT successfully changed operation to importing${WHI}"
else
    echo -e "${RED}$ID_P21IMPORT has operation $OPERATION (should be importing)${WHI}"
    exit 1
fi

STATE=$(grep '^state=' /var/lib/aktin/import/$ID_P21IMPORT/properties | cut -d'=' -f2)
if [[ $STATE == 'successful' ]]; then
    echo -e "${GRE}$ID_P21IMPORT successfully changed state to successful${WHI}"
else
    echo -e "${RED}$ID_P21IMPORT has state $STATE (should be successful)${WHI}"
    exit 1
fi

# # # # # # # # #
# check imported p21 data (should be not zero)
# # # # # # # # #
COUNT_P21=$(sudo -u postgres psql -X -A -d i2b2 -v ON_ERROR_STOP=1 -t -c "SELECT COUNT(provider_id) FROM i2b2crcdata.observation_fact WHERE provider_id='P21'")
if [[ $COUNT_P21 != 0 ]]; then
    echo -e "${GRE}New imported P21 data found (found $COUNT_P21)${WHI}"
else
    echo -e "${RED}}No imported P21 data found${WHI}"
    exit 1
fi

# # # # # # # # #
# chount p21 columns for each imported encounter and compare with predefined
# # # # # # # # #
ARRAY_COUNT_P21=( 28 25 26 24 24 24 26 26 26 25 25 25 25 25 26 26 26 26 26 26 26 0 0 26 26 26 26 26 26 26 0 0 25 25 25 26 26 26 26 26 26 26 26 26 26 26 26 26 25 25 26 25 25 0 0 0 25 25 25 25 26 26 26 26 25 26 25 25 25 25 25 25 25 25 25 25 26 26 26 26 26 26 25 25 25 25 25 25 26 26 26 26 25 25 32 20 20 20 20 33 20 20 33 33 32 32 33 33 33 32 32 33 33 26 26 33 33 32 32 33 33 33 32 32 23 23 26 26 26 23 23 26 26 25 25 23 23 23 23 )
ARRAY_ENC_ID=($(seq 1000 1 1138))
for i in "${!ARRAY_ENC_ID[@]}"
do
    ENC_IDE=$(echo -n "1.2.276.0.76.3.87686/${ARRAY_ENC_ID[$i]}"| openssl sha1 -binary | base64)
    ENC_IDE=$(echo $ENC_IDE | tr \/ _)
    ENC_IDE=$(echo $ENC_IDE | tr + -)

    ENC_NUM=$(sudo -u postgres psql -X -A -d i2b2 -v ON_ERROR_STOP=1 -t -c "SELECT encounter_num FROM i2b2crcdata.encounter_mapping WHERE encounter_ide='$ENC_IDE'")
    if [[ ! -z $ENC_NUM ]]; then
        COUNT_P21=$(sudo -u postgres psql -X -A -d i2b2 -v ON_ERROR_STOP=1 -t -c "SELECT COUNT(provider_id) FROM i2b2crcdata.observation_fact WHERE encounter_num='$ENC_NUM' AND provider_id='P21'")
        if [[ $COUNT_P21 == ${ARRAY_COUNT_P21[$i]} ]]; then
            echo -e "${GRE}Count of Encounter ${ARRAY_ENC_ID[$i]} (Num:$ENC_NUM) is a match${WHI}"
        else
            echo -e "${RED}Count of Encounter ${ARRAY_ENC_ID[$i]} (Num:$ENC_NUM) does not match ($COUNT_P21 instead of ${ARRAY_COUNT_P21[$i]}) ${WHI}"
            exit 1
        fi
    fi
done

# # # # # # # # #
# start file delete of all uploaded files
# # # # # # # # #
for i in "${!ID_SCRIPTS[@]}"
do
    RESPONSE_CODE=$(curl -s -o /dev/null -w '%{http_code}' --location --request DELETE $URL/file/${UUID_SCRIPTS[$i]} --header 'Authorization: Bearer '$BEARER_TOKEN'')
    if [[ $RESPONSE_CODE == 204 ]]; then
	    echo -e "${GRE}DELETE of FILE_$i successful ($RESPONSE_CODE)${WHI}"
    else
	    echo -e "${RED}DELETE of FILE_$i failed ($RESPONSE_CODE)${WHI}"
	    exit 1
    fi
done

# # # # # # # # #
# GETTER for uploaded files (should be zero)
# # # # # # # # #
COUNT_FILES=$(echo $(curl -s --location --request GET $URL/file | grep -o 'id' | wc -l))
if [[ $COUNT_FILES == 0 ]]; then
    echo -e "${GRE}GET file successful (GOT $COUNT_FILES)${WHI}"
else
    echo -e "${RED}GET file failed (GOT $COUNT_FILES)${WHI}"
    echo $(curl -s --location --request GET $URL/file)
    exit 1
fi

# # # # # # # # #
# check for empty aktin/import folder
# # # # # # # # #
if [ -z $(ls -A /var/lib/aktin/import) ]; then
    echo -e "${GRE}/var/lib/aktin/import is emtpy${WHI}"
else
    echo -e "${RED}/var/lib/aktin/import is not emtpy${WHI}"
    exit 1
fi

# # # # # # # # #
# check deleted p21 data
# # # # # # # # #
COUNT_P21=$(sudo -u postgres psql -X -A -d i2b2 -v ON_ERROR_STOP=1 -t -c "SELECT COUNT(provider_id) FROM i2b2crcdata.observation_fact WHERE provider_id='P21'")
if [[ $COUNT_P21 == 0 ]]; then
    echo -e "${GRE}No imported P21 data found${WHI}"
else
    echo -e "${RED}P21 are already imported (found $COUNT_P21)${WHI}"
    exit 1
fi
