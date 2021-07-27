#! /bin/bash

set -euo pipefail

# colors for console output
readonly WHI=${color_white}
readonly RED=${color_red}
readonly ORA=${color_orange}
readonly YEL=${color_yellow}
readonly GRE=${color_green}

# destination url
URL="http://localhost:80/aktin/admin/rest"

# login into aktin/admin and get bearer-token
BEARER_TOKEN=$(curl -s --location --request POST ''$URL'/auth/login/' --header 'Content-Type: application/json' --data-raw '{ "username": "i2b2", "password": "demouser" }')

# create random string and number
RANDOM_STRING=$(echo $(cat /dev/urandom | tr -dc 'a-zA-Z' | fold -w 6 | head -n 1))
RANDOM_NUMBER=$(echo $(cat /dev/urandom | tr -dc '0-9' | fold -w 6 | head -n 1))

# try post on aktin/admin/consentManager via token and get response code
RESPONSE_CODE=$(curl -s -o /dev/null -w "%{http_code}" --location --request POST ''$URL'/optin/AKTIN/Patient/1.2.276.0.76.4.8/'$RANDOM_NUMBER'' --header 'Authorization: Bearer '$BEARER_TOKEN'' --header 'Content-Type: application/json' --data-raw '{ "opt": 1, "sic": "", "comment": "'$RANDOM_STRING'" }')

# check if response code is 200, print whole response on failure (via new request)
if [[ $RESPONSE_CODE == 200 || $RESPONSE_CODE == 201 ]]; then
	echo -e "${GRE}Test consent-manager successful ($RESPONSE_CODE)${WHI}"
else
	echo -e "${RED}Test consent-manager ($RESPONSE_CODE)${WHI}"
	echo $(curl -s --location --request POST ''$URL'/optin/AKTIN/Patient/1.2.276.0.76.4.8/'$RANDOM_NUMBER'' --header 'Authorization: Bearer '$BEARER_TOKEN'' --header 'Content-Type: application/json' --data-raw '{ "opt": 1, "sic": "", "comment": "'$RANDOM_STRING'" }')
	exit 1
fi
