#!/bin/bash

if [ -z "$1" ] ; then
  echo "Error: Missing required arguments."
  echo "Usage: $0 <email> [<host_and_port>]"
  echo "Example: $0 user@example.com"
  echo "host_and_port defaults to syracuse.1145.am"
  exit 1
fi

EMAIL="$1"
HOST_AND_PORT=${2:-syracuse.1145.am}
DATA="{\"email\":\"$EMAIL\"}"

echo "Registering $EMAIL with $HOST_AND_PORT"

curl -X POST "http://$HOST_AND_PORT/api/v1/register-and-get-key/" \
  -H "Content-Type: application/json" \
  -d "$DATA"