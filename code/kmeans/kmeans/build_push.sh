#!/bin/bash

# Build the image
$SPARK_HOME/bin/docker-image-tool.sh -r gabrielestentella -t km -f /Users/gabriele/Desktop/TesiStentella/tesistentella/kmeans/kmeans/Dockerfile build

# Push the image
docker push gabrielestentella/spark:km