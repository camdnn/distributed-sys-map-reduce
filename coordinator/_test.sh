#!/bin/bash
./_clean_intermediate.sh

go run . &

sleep 10 

for i in $(seq 1 5);
do 
    go run ../worker &
done