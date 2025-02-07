SHELL = /bin/bash

.PHONY: build-base-image create-deps-archive run
	

build-base-image:
	docker build --tag spark:3.5.3-python-extra-jars --target production ./docker


define ARCHIVE_SHELL_COMMAND =
  	rm -rf pyspark_venv ${TASK_NAME}.tar.gz ; \
	python3 -m venv pyspark_venv ; \
	source pyspark_venv/bin/activate ; \
	pip install -r requirements.txt ; \
	# ( cd pyspark_venv/lib/python3.8/site-packages && zip -qr ../../../../${TASK_NAME}.zip .; )
	# venv-pack -o ${TASK_NAME}.tar.gz
endef
# Parameters: \
# - TASK_NAME: folder name of the task whose deps will be packaged
create-deps-archive: build-base-image
	test -n "${TASK_NAME}" || { echo "**Error**: You did not specify the TASK_NAME parameter" && exit 10; }
	chmod o+rw ${TASK_NAME} # necessary to create new files/folders inside the task folder
	docker run --rm -v "./${TASK_NAME}:/opt/spark/work-dir" spark:3.5.3-python-extra-jars bash -c "${ARCHIVE_SHELL_COMMAND}"


define RUN_SHELL_COMMAND =
  	export PYSPARK_DRIVER_PYTHON=python3 && \
	export PYSPARK_PYTHON=./venv/bin/python && \
	\$$SPARK_HOME/bin/spark-submit --archives ${TASK_NAME}.tar.gz#venv \
		mnist_example.py
endef
# Parameters: \
# - TASK_NAME: folder name of the task whose deps will be packaged
run:
	docker run --rm -v "./${TASK_NAME}:/opt/spark/work-dir" spark:3.5.3-python-extra-jars bash -c "${RUN_SHELL_COMMAND}"


shell:
	$$SPARK_HOME/bin/pyspark --archives output/sample-tsf.tar.gz#venv