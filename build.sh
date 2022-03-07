#!/bin/bash

git pull origin $version

# docker build --build-arg APP_NAME=tone-runner --build-arg ENV=daily -f docker/Dockerfile -t tone-runner:v1.0.0 .
