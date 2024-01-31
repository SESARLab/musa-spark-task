FROM amazoncorretto:8
ADD kmeans_jar/ /
ADD dataset.csv /

CMD ["/usr/lib/jvm/java-1.8.0-amazon-corretto/bin/java", "-jar", "kmeans.jar", "./dataset.csv", "3", "./RESULT"]
