#!/bin/bash

# Get today's date in YYYYMMDD format
today=$(date +"%Y%m%d")

# Write it into dco.txt (overwrite each time)
echo "$today" > dco.txt

echo "✅ dco.txt generated with today's date: $today"
