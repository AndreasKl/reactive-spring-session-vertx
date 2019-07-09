#!/bin/bash

for i in {1..5000} 
do
  echo "loop $i"
  sudo ss -K dst 127.0.0.1 dport = 39889
  sleep 0.1

done
