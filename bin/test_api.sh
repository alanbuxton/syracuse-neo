
#!/bin/bash

if [ -z "$1" ] || [ -z "$SYRACUSE_API_KEY" ]; then
  echo "Error: Missing required arguments."
  echo "Usage: $0 <org_name_to_search_for> [<host_and_port>]"
  echo "SYRACUSE_API_KEY env var must be set"
  echo "host_and_port defaults to syracuse.1145.am"
  echo "Example: SYRACUSE_API_KEY=my_key $0 tesla"
  echo "Activities API documentation: https://syracuse.1145.am/api/schema/swagger-ui/#/activities/activities_list"
  exit 1
fi

ORG_NAME=$1
HOST_AND_PORT=${2:-syracuse.1145.am}

curl -X 'GET' \
  "http://$HOST_AND_PORT/api/v1/activities/?org_name=$ORG_NAME" \
  -H "Accept: */*" -H "Authorization: Token $SYRACUSE_API_KEY"
