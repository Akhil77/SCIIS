#!/bin/sh
docker build -t final-worker .
docker tag final-worker akhilsm/final-worker:v1
docker push akhilsm/final-worker:v1

#kubectl create deployment worker --image=gcr.io/dcsc-3/worker