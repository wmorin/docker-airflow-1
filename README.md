# cryoem-airflow

This repo contains [airflow](https://pypi.python.org/pypi/apache-airflow) based workflows for cryoem data collection and analysis at the [SLAC National Accelerator Center](https://sites.slac.stanford.edu/cryo-em/).

Specifically, the airflow workflows do:

- Data Moving: copying the raw data and metadata from the cryoem hardware over to long term GPFS storage
- Pre-processing: runs initial CTF calculations and alignment of images; logs and reports data to a timeseries datastore, the cryoem elogbook and slack.

## Pre-requisites

A docker-compose configuration file is provided to facilitate deployment.

### Setup Host

The relevant CIFS and GPFS mounts should be created on all nodes that will participate for the data acquisition (DAQ); eg

    # install CIFS
    yum install -y samba-client samba-common cifs-utils

    # setup credentias for CIFS
    cat <<EOF > /etc/samba/tem.creds
    username=<user_for_tem>
    password=<password_for_tem>
    EOF
    # ensure permissions
    chmod go-rwx /etc/samba/tem.creds

    # setup CIFS mountpoint and options
    cat <<EOF >> /etc/fstab

    # cryoem TEM
    # mount
    mkdir -p /srv/cryoem/tem1
    mkdir -p /srv/cryoem/tem2
    mkdir -p /srv/cryoem/tem3
    mkdir -p /srv/cryoem/tem4
    
    # edit fstab for persistence
    cat <<EOF >> /etc/fstab

    # TEM mountpoints
    //<ip_of_tem>/data    /srv/cryoem/tem1/ cifs uid=<cryoem_user>,gid=<cryoem_group>,forceuid,forcegid,dom=<domainname_of_tem>,file_mode=0777,dir_mode=0777,noperm,credentials=/etc/samba/tem.creds 0 0
    EOF


### Setup Repository and Files

    cd
    # get the code
    git clone https://github.com/slaclab/cryoem-airflow.git cryoem-airflow
    # setup persistence
    mkdir data/
    mkdir data/postgres
    mkdir data/redis
    mkdir data/logs
    # setup cryoem experiments
    mkdir -p experiment/tem1
    cat > experiment/tem1/tem1-experiment.yaml << EOF
experiment:
  name: 20171204_sroh-hsp
  microscope: krios1
  fmdose: 1.2
EOF    

### Setup Docker

This work utilises [apache-airflow](https://github.com/apache/incubator-airflow) for [Docker](https://www.docker.com/). It is based on the work from [puckel's docker-airflow](https://github.com/puckel/docker-airflow) and uses the official [Postgres](https://hub.docker.com/_/postgres/) as backend and [Redis](https://hub.docker.com/_/redis/) as queue.

    # install pre-reqs
    yum install -y epel-release python-pip wget git
    pip install -U pip
    yum  -y remove  docker-common docker container-selinux docker-selinux docker-engine
    wget https://download.docker.com/linux/centos/docker-ce.repo -O /etc/yum.repos.d/docker-ce.repo
    yum -y install docker-ce
    
    # optional - setup user privs for docker:
    sudo usermod -aG docker <userid>
    
    # on all nodes
    sudo systemctl restart docker
    sudo systemctl enable docker
    
We make use of [docker swarm](https://docs.docker.com/engine/swarm/) to provide quick deployment and scalability.

    sudo docker swarm init --force-new-cluster
    
    # on the other nodes
    docker swarm join --token <token_from_swarm> <ip_of_swarm_master>:2377
    
We choose to run 3 managers and 5 workers.

For local testing, you may choose to use [Docker Compose](https://docs.docker.com/compose/install/) instead.


## Installation and Usage

We may(?) need to build the image first:

    docker build --rm -t slaclab/cryoem-airflow:1.8.2 .

    # push to dockerhub
    docker login
    docker push slaclab/cryoem-airflow:1.8.2

Now that everything should be setup, let's start the airflow containers:

    cd cryoem-airflow
    docker stack deploy  --prune -c docker-compose.yaml cryoem-airflow

After a little time, all of the services should be up:

    $ docker stack ls
    NAME                SERVICES
    cryoem-airflow      6
    
    $ docker stack ps cryoem-airflow
    ID                  NAME                         IMAGE                          NODE                DESIRED STATE       CURRENT STATE            ERROR               PORTS
    troui5fwarqr        cryoem-airflow_postgres.1    postgres:9.6                   cryoem-daq03        Running             Running 7 seconds ago
    90ciwxm0ndvb        cryoem-airflow_redis.1       redis:3.2.7                    cryoem-daq01        Running             Running 28 seconds ago
    qkuj38s0vr94        cryoem-airflow_worker.1      slaclab/cryoem-airflow:1.8.2   cryoem-daq01        Running             Running 29 seconds ago
    zwj1waucgag2        cryoem-airflow_scheduler.1   slaclab/cryoem-airflow:1.8.2   cryoem-daq01        Running             Running 31 seconds ago
    i7u36z7qt0gu        cryoem-airflow_flower.1      slaclab/cryoem-airflow:1.8.2   cryoem-daq03        Running             Running 32 seconds ago
    30w8vsm7nxvp        cryoem-airflow_webserver.1   slaclab/cryoem-airflow:1.8.2   cryoem-daq02        Running             Running 34 seconds ago
    lafftqw1psfh        cryoem-airflow_worker.2      slaclab/cryoem-airflow:1.8.2   cryoem-daq03        Running             Running 30 seconds ago
    s62hnw3p6k1g        cryoem-airflow_worker.3      slaclab/cryoem-airflow:1.8.2   cryoem-daq02        Running             Running 29 seconds ago


Check [Airflow Documentation](https://pythonhosted.org/airflow/)

You should then be able to goto [localhost:8080](http://localhost:8080/) and see all of the workflows.


## Technical Details

The workflows are python scripts placed under `dags`. One can also install reusable operator and sensors under `plugins`.

### file-drop

The `file_drop.py` DAGs reads in experimental setup information from a yaml file and sets up the storage in preparation for the rsyncing of the data from the TEM to our long term file store. It also deletes old data from the TEM.

### pre-processing




## Notes


## Install custom python package

- Create a file "requirements.txt" with the desired python modules
- Mount this file as a volume `-v $(pwd)/requirements.txt:/requirements.txt`
- The entrypoint.sh script execute the pip install command (with --user option)

## UI Links

- Airflow: [localhost:8080](http://localhost:8080/)
- Flower: [localhost:5555](http://localhost:5555/)


