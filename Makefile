TAG=20200405.0

server:
	sudo docker build . -t slaclab/cryoem-airflow:${TAG}
	sudo docker push slaclab/cryoem-airflow:${TAG}

worker:
	sudo docker build . -f Dockerfile.worker -t slaclab/cryoem-airflow-worker:${TAG}
	sudo docker push slaclab/cryoem-airflow-worker:${TAG}
	sudo singularity pull docker://slaclab/cryoem-airflow-worker:${TAG}
	mv cryoem-airflow-worker_${TAG}.sif /gpfs/slac/cryo/fs1/daq/dev/airflow/bin/cryoem-airflow-worker\@${TAG}.sif


