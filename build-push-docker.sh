#!/bin/bash
set -e

IMAGE_NAME="ghcr.io/pharmbio/s3uploader"

docker build -t $IMAGE_NAME:latest .
docker push $IMAGE_NAME:latest

read -p "Do you want to tag and push this as \"stable\"? [y/N] " -n 1 -r < /dev/tty
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    docker tag $IMAGE_NAME:latest $IMAGE_NAME:stable
    docker push $IMAGE_NAME:stable
fi
