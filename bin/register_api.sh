#!/bin/bash

if [ -z "$1" ] ; then
  echo "Error: Missing required arguments."
  echo "Usage: $0 <email> [<scheme_host_and_port>]"
  echo "Example: $0 user@example.com"
  echo "scheme_host_and_port defaults to https://syracuse.1145.am"
  exit 1
fi

EMAIL="$1"
SCHEME_HOST_AND_PORT=${2:-https://syracuse.1145.am}
DATA="{\"email\":\"$EMAIL\"}"

echo "Registering $EMAIL with $SCHEME_HOST_AND_PORT"

curl -X POST "$SCHEME_HOST_AND_PORT/api/v1/register-and-get-key/" \
  -H "Content-Type: application/json" \
  -d "$DATA"
