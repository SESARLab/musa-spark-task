#!/bin/bash

$SPARK_HOME/bin/spark-submit \
  --master k8s://https://kubernetes.docker.internal:6443 \
  --deploy-mode cluster \
  --name kmeans \
  --class it.unimi.evotion.tasks.kmeans.Kmeans \
  --conf spark.executor.instances=5 \
  --conf spark.kubernetes.container.image=gabrielestentella/spark:km \
  --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
  local:///opt/spark/jars/kmeans-1.0-jar-with-dependencies.jar ../dataset/dataset.csv 5 ../RESULT

  $SPARK_HOME/bin/spark-submit \
  --master k8s://https://kubernetes.docker.internal:6443 \
  --deploy-mode cluster \
  --name kmeans \
  --class it.unimi.evotion.tasks.kmeans.Kmeans \
  --conf spark.kubernetes.container.image=eclipse-temurin:8-jre \
  --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
  --conf spark.executor.instances=5 \
  s3://eks-blog-us-east-1-654654215663/kmeans-1.0-jar-with-dependencies.jar s3://eks-blog-us-east-1-654654215663/dataset.csv 5 s3://eks-blog-us-east-1-654654215663/RESULT