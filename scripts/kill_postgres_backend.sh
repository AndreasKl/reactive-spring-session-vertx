#!/bin/bash

for i in {1..5000} 
do
  echo "loop $i"
  psql -h localhost -p 39889 -U postgres \
       -c "SELECT pg_terminate_backend(pg_stat_activity.pid) FROM pg_stat_activity WHERE pg_stat_activity.datname = 'session' AND pid <> pg_backend_pid();"
  sleep 0.1

done
