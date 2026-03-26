#!/bin/bash

NN="nn"
HADOOP_USER="hadoop"
SCRIPT="script.py"
VENV_DIR="hw_5/.venv"
WORK_DIR="hw_5"

echo "Creating venv and installing dependencies on ${NN}..."
ssh ${HADOOP_USER}@${NN} "
    mkdir -p ~/${WORK_DIR} &&
    python3 -m venv ~/${VENV_DIR} &&
    ~/${VENV_DIR}/bin/pip install --quiet --upgrade pip &&
    ~/${VENV_DIR}/bin/pip install --quiet prefect pyspark
"

echo "Copying ${SCRIPT} to ${HADOOP_USER}@${NN}:~/${WORK_DIR}/"
scp ${SCRIPT} ${HADOOP_USER}@${NN}:~/${WORK_DIR}/${SCRIPT}

echo "Launching ${SCRIPT} on ${NN}..."
ssh ${HADOOP_USER}@${NN} "
    source ~/.profile &&
    export HADOOP_CONF_DIR=/home/hadoop/hadoop-3.4.1/etc/hadoop &&
    export YARN_CONF_DIR=/home/hadoop/hadoop-3.4.1/etc/hadoop &&
    ~/${VENV_DIR}/bin/python3 ~/${WORK_DIR}/${SCRIPT}
"

echo "Done."