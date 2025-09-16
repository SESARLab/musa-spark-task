SHELL = /bin/bash
JAVA_OUT_FOLDER := output/java

.PHONY: init create-extra-deps-jar create-task-jar


init: 
	mkdir -p ${JAVA_OUT_FOLDER}


create-extra-deps-jar: init
	( cd java && mvn install -Pextra-only; ) && \
	cp java/extra-jars/target/extra-jars-1.0.0-jar-with-dependencies.jar ${JAVA_OUT_FOLDER}


create-task-jar: init
	( cd java && mvn package -Ptasks; ) && \
	cp java/lr/lr/target/lr-1.0-jar-with-dependencies.jar \
		java/svm/svm/target/svm-1.0-jar-with-dependencies.jar \
		java/kmeans/kmeans/target/kmeans-1.0-jar-with-dependencies.jar \
		java/example/target/example-1.0.0-jar-with-dependencies.jar \
		java/anova/target/anova-1.0-jar-with-dependencies.jar \
		java/bubblechart/target/bubblechart-1.0-jar-with-dependencies.jar \
		java/minmaxScaler/target/minmaxscaler-1.0-jar-with-dependencies.jar \
		${JAVA_OUT_FOLDER}


