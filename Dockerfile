FROM amazoncorretto:8

ARG path
ARG dataset
ADD $path /jars
ADD $dataset /
