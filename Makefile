ENVIRONMENT := $(word 1, $(ENVIRONMENT))

install:
	git submodule update --init --recursive
	pip3 install "apache-airflow[crypto,celery,postgres,hive,jdbc,ssh]==2.2.5" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.2.5/constraints-3.8.txt"
	pip3 install -r python-tools/requirements.txt
	pip3 install -r aiq-dynamo-python/requirements.txt

py-install:
#	Just installs python modules
#	Used for local build and testing
	pip3 install pipdeptree
	pip3 install "apache-airflow[crypto,celery,postgres,hive,jdbc,ssh]==2.2.5" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.2.5/constraints-3.8.txt"
	pip3 install -r requirements.txt
	pip3 install -r python-tools/requirements.txt
	pip3 install -r aiq-dynamo-python/requirements.txt

uninstall:
#	uninstall pke specifically, due to github specific link
	pip3 uninstall pke -y
	pip3 freeze | xargs pip uninstall -y
	pip3 cache purge

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
