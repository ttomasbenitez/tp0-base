#!/bin/bash

SERVER="server:12345"
MESSAGE="Hello"

RESPONSE=$(docker run --rm --network testing_net busybox:latest sh -c "echo '$MESSAGE' | nc $SERVER")

if [[ "$RESPONSE" == "$MESSAGE" ]]; then
  echo "action: test_echo_server | result: success"
else
  echo "action: test_echo_server | result: fail"
fi
