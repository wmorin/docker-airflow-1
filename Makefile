ENVIRONMENT := $(word 1, $(ENVIRONMENT))

install:
	git submodule update --init --recursive
	pip install airflow
	pip install -r python-tools/requirements.txt
	pip install -r aiq-dynamo-python/requirements.txt

lint:
	flake8 --max-line-length=137 --exclude venv,generated,lib,env,test_env,.git,.venv,python-tools,aiq-dynamo-python

type-check:
	flake8 --max-line-length=137

test-local:
	find tests/ -name \*.pyc -delete
	pytest -vv

test:
	PYTHONPATH=$PYTHONPATH:. pytest --ignore=python-tools

run-local:
	docker-compose -f docker-compose-LocalExecutor.yml up

checkout:
	git submodule sync
	git submodule update --init --recursive

update:
	git submodule update --remote --merge

#########################################################
##### Used by Jenkins ###################################

lint-in-docker:
	docker run ${DOCKER_REV} flake8 --max-line-length=137

type-check-in-docker:
	docker run ${DOCKER_REV} flake8 --max-line-length=137
