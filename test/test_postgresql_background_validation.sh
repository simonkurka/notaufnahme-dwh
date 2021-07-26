#! /bin/bash

set -euo pipefail

readonly INTEGRATION_ROOT=$(pwd)/resources
readonly XML_FILES=$INTEGRATION_ROOT/xml

# colors for console output
readonly WHI=${color_white}
readonly RED=${color_red}
readonly ORA=${color_orange}
readonly YEL=${color_yellow}
readonly GRE=${color_green}

if ! systemctl is-active --quiet postgresql; then
	echo "Starting postgresql"
	service postgresql start
fi

if ! systemctl is-active --quiet wildfly; then
	echo "Starting wildfly"
	service wildfly start
	sleep 10s
fi

echo "Stopping postgresql"
service postgresql stop
sleep 5s
if ! systemctl is-active --quiet postgresql; then
	echo -e "${GRE}postgresql is stopped${WHI}"
else
	echo -e "${RED}postgresql is not stopped${WHI}"
fi
if ! systemctl is-active --quiet wildfly; then
	echo -e "${GRE}wildfly is stopped${WHI}"
else
	echo -e "${RED}wildfly is not stopped${WHI}"
fi

echo "Starting widlfly"
service wildfly start
sleep 5s
if systemctl is-active --quiet wildfly; then
	echo -e "${GRE}wildfly is started${WHI}"
else
	echo -e "${RED}wildfly is not started${WHI}"
fi
if systemctl is-active --quiet postgresql; then
	echo -e "${GRE}postgresql is started${WHI}"
else
	echo -e "${RED}postgresql is not started${WHI}"
fi
