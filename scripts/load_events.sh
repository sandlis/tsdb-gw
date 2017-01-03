#!/bin/bash

function usage() {
	echo "$0 --addr <ES Address> --org <OrgId> --endpoint <endpointSlug> --probe <probeSlug> --type <type (ping|dns|http|https)> --count <count of events>" 
}

LONG=addr:,org:,endpoint:,probe:

PARSED=`getopt -o h --longoptions $LONG --name "$0" -- "$@"`
if [ $? -ne 0 ]; then
    # e.g. $? == 1
    #  then getopt has complained about wrong arguments to stdout
    exit 2
fi
# use eval with "$PARSED" to properly handle the quoting
eval set -- "$PARSED"

ADDR="localhost:9200"
ORG="1"
ENDPOINT="google.com"
PROBE="new-york"
TYPE="ping"
COUNT=10

# now enjoy the options in order and nicely split until we see --
while true; do
    case "$1" in
    	-h)
			usage
			exit 0
			;;
        --addr)
            ADDR=$2
            shift 2
            ;;
        --org)
            ORG=$2
            shift 2
            ;;
        --endpoint)
            ENDPOINT=$2
            shift 2
            ;;
        --probe)
            PROBE=$2
            shift 2
            ;;
        --type)
            TYPE=$2
            shift 2
            ;;
        --count)
            COUNT=$2
            shift 2
            ;;
        --)
            shift
            break
            ;;
        *)
            echo "Programming error"
            exit 3
            ;;
    esac
done


read -d '' DOC_TEMPLATE << EOF
{
	"id": "%%ID%%",
	"event_type": "monitor_state",
	"org_id": $ORG,
	"severity": "ERROR",
	"source": "monitor_collector",
	"timestamp": %%TS%%,
	"message": "Error event",
	"tags": {
	    "collector": "$PROBE",
	    "endpoint": "$ENDPOINT",
	    "monitor_type": "$TYPE"
	}
}
EOF

for i in $(seq 1 $COUNT); do
	TS="$(date +%s)000"
	PAYLOAD=$(echo $DOC_TEMPLATE | sed -e "s/%%ID%%/$(uuidgen)/" -e "s/%%TS%%/$TS/")
	curl http://$ADDR/events-$(date +%F -u)/monitor_state -X POST -d "$PAYLOAD"
done