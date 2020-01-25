#!/bin/bash

container=$1
server=$2
user=$3
password=$4

counter=1
while true
do
  if docker exec -it "$container" /opt/mssql-tools/bin/sqlcmd -S "$server" -U "$user" -P "$password" -Q "SELECT @@VERSION"; then
    exit 0  # success
  fi
  if [ $counter -gt 3 ]; then
    echo "MSSQL apparently not available"
    exit 1
  fi
  echo "Call to MSSQL failed, trying again in 5 seconds..."
  sleep 5
  ((counter++))
done
