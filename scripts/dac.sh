#!/bin/bash -e
set -o pipefail


echo "Starting DAC..."
python main.py scrape &
PID1=$!
flask run --host 0.0.0.0 &
PID2=$!
echo "Started DAC."


function cleanup() {
  set +e
  echo "Stopping DAC..."
  kill "${PID2}"
  kill "${PID1}"
  echo "Stopped DAC."
}

trap cleanup EXIT

wait