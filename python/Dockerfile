# syntax=docker/dockerfile:1

FROM spark:3.5.3-python3 AS production

ARG HADOOP_VERSION="3.3.4"

USER root

RUN <<EOF
    apt update
    apt install -y python3.8-venv
EOF

USER spark

# install dependencies to read/write on S3-compatible
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-3.3.4.jar /opt/spark/jars/hadoop-aws-${HADOOP_VERSION}.jar
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/${HADOOP_VERSION}/hadoop-common-3.3.4.jar /opt/spark/jars/hadoop-common-${HADOOP_VERSION}.jar

FROM production AS devcontainer

ARG REMOTE_USER
ARG REMOTE_UID
ARG REMOTE_GID

USER root

RUN <<EOF
    addgroup --gid ${REMOTE_GID} ${REMOTE_USER}
    adduser --disabled-password --uid ${REMOTE_UID} --gid ${REMOTE_GID} ${REMOTE_USER}
EOF

USER ${REMOTE_USER}