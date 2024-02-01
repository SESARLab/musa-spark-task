FROM amazoncorretto:8

ARG jarpath
ARG dataset
ARG modeldata
ARG modeldir

ADD $jarpath /jars
ADD $dataset /
ADD $modeldata $modeldir
