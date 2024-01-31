#! /bin/bash
docker volume create res
docker build --no-cache --progress=plain -t kmeans-container-image .
docker run kmeans-container-image
