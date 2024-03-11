#!/bin/bash


# Array to store PIDs
pids=()

# Function to trap SIGINT (Ctrl+C) and stop instances
trap 'kill ${pids[@]}' SIGINT

# Loop to run 50 instances
for ((i=1; i<=7; i++)); do
    cargo run &   # Execute the application in the background and capture PID
    pids+=($!)             # Store the PID of the last background process
done

# Wait for all instances to finish
wait
