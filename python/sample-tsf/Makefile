SHELL = /bin/bash

.PHONY: init create-archive


init: 
	mkdir -p output

create-archive: init
	source pyspark_venv/bin/activate && \
	pip install -r requirements.txt && \
	venv-pack -o output/sample-tsf.tar.gz
