#!/usr/bin/env bash
#   ./homework4_setup.sh

set -e

GATEWAY="${GATEWAY:-178.236.25.105}"
SSH_KEY="${SSH_KEY:-.ssh/your_key}"
[[ "$SSH_KEY" != /* ]] && SSH_KEY="${HOME}/${SSH_KEY}"
UBUNTU_PASSWORD="${UBUNTU_PASSWORD:-password}"
NN_HOST="${NN_HOST:-nn}"

if [[ ! -f "$SSH_KEY" ]]; then
    echo "Ключ не найден: $SSH_KEY"
    echo "Задайте путь: SSH_KEY=/path/to/key ./homework4_setup.sh"
    exit 1
fi

SSH="ssh -i $SSH_KEY -o StrictHostKeyChecking=no ubuntu@$GATEWAY"
SCP="scp -i $SSH_KEY -o StrictHostKeyChecking=no"

run_as_hadoop() {
    local cmd="$1"
    $SSH "echo '$UBUNTU_PASSWORD' | sudo -S -i -u hadoop -- bash -c $(printf '%q' "$cmd")"
}

run_as_hadoop "cd /home/hadoop && wget -q https://archive.apache.org/dist/spark/spark-3.5.3/spark-3.5.3-bin-hadoop3.tgz && tar -xzf spark-3.5.3-bin-hadoop3.tgz && echo 'Spark установлен'"

PROFILE_BLOCK='export SPARK_DIST_CLASSPATH="/home/hadoop/spark-3.5.3-bin-hadoop3/jars/*:/home/hadoop/hadoop-3.4.1/etc/hadoop:/home/hadoop/hadoop-3.4.1/share/hadoop/common/lib/*:/home/hadoop/hadoop-3.4.1/share/hadoop/common/*:/home/hadoop/hadoop-3.4.1/share/hadoop/hdfs:/home/hadoop/hadoop-3.4.1/share/hadoop/hdfs/lib/*:/home/hadoop/hadoop-3.4.1/share/hadoop/hdfs/*:/home/hadoop/hadoop-3.4.1/share/hadoop/mapreduce/*:/home/hadoop/hadoop-3.4.1/share/hadoop/yarn:/home/hadoop/hadoop-3.4.1/share/hadoop/yarn/lib/*:/home/hadoop/hadoop-3.4.1/share/hadoop/yarn/*:/home/hadoop/apache-hive-4.0.0-alpha-2-bin/*:/home/hadoop/apache-hive-4.0.0-alpha-2-bin/lib/*"
export SPARK_HOME="/home/hadoop/spark-3.5.3-bin-hadoop3/"
'
TMP_PROFILE=$(mktemp)
trap "rm -f $TMP_PROFILE" EXIT
echo "$PROFILE_BLOCK" > "$TMP_PROFILE"
$SCP "$TMP_PROFILE" "ubuntu@${GATEWAY}:/tmp/spark_profile_add.txt"
run_as_hadoop "grep -q 'SPARK_HOME=' /home/hadoop/.profile 2>/dev/null || cat /tmp/spark_profile_add.txt >> /home/hadoop/.profile"
$SSH "rm -f /tmp/spark_profile_add.txt"

run_as_hadoop "scp -o StrictHostKeyChecking=no /home/hadoop/.profile ${NN_HOST}:/home/hadoop/"
echo ".profile скопирован на nn"

run_as_hadoop "ssh -o StrictHostKeyChecking=no ${NN_HOST} 'pkill -f \"hive.*metastore\" 2>/dev/null; sleep 1; nohup hive --hiveconf hive.server2.enable.doAs=false --hiveconf hive.security.authorization.enabled=false --service metastore >> /tmp/hms.log 2>&1 &'"
sleep 10

run_as_hadoop "python3 -m venv /home/hadoop/.venv 2>/dev/null" || {
    $SSH "echo '$UBUNTU_PASSWORD' | sudo -S apt-get update -qq && echo '$UBUNTU_PASSWORD' | sudo -S apt-get install -y python3.12-venv"
    run_as_hadoop "python3 -m venv /home/hadoop/.venv"
}

run_as_hadoop 'source /home/hadoop/.venv/bin/activate && pip install -U pip -q && pip install onetl ipython pyspark==3.5.3 -q && echo "Пакеты установлены"'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYSCRIPT="${SCRIPT_DIR}/homework4_spark_job.py"
if [[ -f "$PYSCRIPT" ]]; then
    $SCP "$PYSCRIPT" "ubuntu@${GATEWAY}:/tmp/homework4_spark_job.py"
    $SSH "echo '$UBUNTU_PASSWORD' | sudo -S -i -u hadoop -- cp /tmp/homework4_spark_job.py /home/hadoop/homework4_spark_job.py"
    run_as_hadoop 'source /home/hadoop/.venv/bin/activate && source /home/hadoop/.profile 2>/dev/null; export PATH="/home/hadoop/spark-3.5.3-bin-hadoop3/bin:$PATH"; cd /home/hadoop && ipython --no-banner homework4_spark_job.py'
else
    echo "Файл homework4_spark_job.py не найден: $PYSCRIPT"
    echo "Скопируйте его на jn в /home/hadoop/ и на jn выполните от hadoop: ipython homework4_spark_job.py"
    exit 1
fi

echo "  ssh -i $SSH_KEY ubuntu@$GATEWAY"
echo "  sudo -i -u hadoop"
echo "  beeline -u jdbc:hive2://jn:5433 -n scott tiger"
echo "  SHOW TABLES;"
echo "  SELECT * FROM sales_aggregated;"
