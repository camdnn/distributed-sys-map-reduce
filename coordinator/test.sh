#!/bin/bash
go run . &

sleep 2 

for i in $(seq 1 3):
do 
    go run ../worker;
done