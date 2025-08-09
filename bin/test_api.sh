
#!/bin/bash

if [ -z "$1" ] || [ -z "$SYRACUSE_API_KEY" ]; then
  echo "Error: Missing required arguments."
  echo "Usage: $0 <org_name_to_search_for> [<scheme_host_and_port>]"
  echo "SYRACUSE_API_KEY env var must be set"
  echo "scheme_host_and_port defaults to https://syracuse.1145.am"
  echo "Example: SYRACUSE_API_KEY=my_key $0 tesla"
  echo "Activities API documentation: https://syracuse.1145.am/api/schema/swagger-ui/#/activities/activities_list"
  exit 1
fi

ORG_NAME=$1
SCHEME_HOST_AND_PORT=${2:-https://syracuse.1145.am}

urlencode() {
    local str="$1"
    local length=${#str}
    local encoded=""

    for (( i = 0; i < length; i++ )); do
        local c="${str:i:1}"
        case "$c" in
            [a-zA-Z0-9.~_-]) encoded+="$c" ;;
            ' ') encoded+='%20' ;;
            *) printf -v hex '%%%02X' "'$c"
               encoded+="$hex"
               ;;
        esac
    done
    echo "$encoded"
}

ENCODED_ORG=$(urlencode "$ORG_NAME")

curl -X 'GET' \
  "$SCHEME_HOST_AND_PORT/api/v1/activities/?org_name=$ENCODED_ORG" \
  -H "Accept: */*" -H "Authorization: Token $SYRACUSE_API_KEY"
