ENVIRONMENT := $(word 1, $(ENVIRONMENT))

install:
	pip3 install -r requirements.txt

lint:
	flake8 --max-line-length=137

type-check:
	flake8 --max-line-length=137

test-local:
	find tests/ -name \*.pyc -delete
	pytest -vv

test:
	PYTHONPATH=$PYTHONPATH:. pytest

run-local:
	docker-compose -f docker-compose-LocalExecutor.yml up

#########################################################
##### Used by Jenkins ###################################

lint-in-docker:
	docker run ${DOCKER_REV} flake8 --max-line-length=137

type-check-in-docker:
	docker run ${DOCKER_REV} flake8 --max-line-length=137
