#!/bin/bash
set -e

# --- Переменные ---
HADOOP_CONF_DIR="/home/hadoop/hadoop-3.4.1/etc/hadoop"
HADOOP_HOME="/home/hadoop/hadoop-3.4.1"

NODES=("nn" "dn-00" "dn-01")

CONFIG_FILES=("mapred-site.xml" "yarn-site.xml")
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# --- Функции логирования ---
log_info()  { echo -e "${GREEN}[INFO]${NC}  $1"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC}  $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }
log_step()  { echo -e "\n${BLUE}======== $1 ========${NC}"; }

# Создание конфигурационных файлов
create_config_files() {
    log_step "Создание конфигурационных файлов"

    # ---------- mapred-site.xml ----------
    cat > "${SCRIPT_DIR}/mapred-site.xml" << 'EOF'
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<!--
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

<!-- Put site-specific property overrides in this file. -->

<configuration>
    <property>
        <name>mapreduce.framework.name</name>
        <value>yarn</value>
    </property>
    <property>
        <name>mapreduce.application.classpath</name>
        <value>$HADOOP_HOME/share/hadoop/mapreduce/*:$HADOOP_HOME/share/hadoop/mapreduce/lib/*</value>
    </property>
</configuration>
EOF

    # ---------- yarn-site.xml ----------
    cat > "${SCRIPT_DIR}/yarn-site.xml" << 'EOF'
<?xml version="1.0"?>
<!--
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->
<configuration>
    <property>
        <name>yarn.nodemanager.aux-services</name>
        <value>mapreduce_shuffle</value>
    </property>
    <property>
        <name>yarn.nodemanager.env-whitelist</name>
        <value>JAVA_HOME,HADOOP_COMMON_HOME,HADOOP_HDFS_HOME,HADOOP_CONF_DIR,CLASSPATH_PREPEND_DISTCACHE,HADOOP_YARN_HOME,PATH,LANG,TZ,HADOOP_MAPRED_HOME</value>
    </property>
    <property>
        <name>yarn.resourcemanager.hostname</name>
        <value>nn</value>
    </property>
    <property>
        <name>yarn.resourcemanager.address</name>
        <value>nn:8032</value>
    </property>
    <property>
        <name>yarn.resourcemanager.resource-tracker.address</name>
        <value>nn:8031</value>
    </property>
</configuration>
EOF

    log_info "Файлы созданы в: ${SCRIPT_DIR}"
}

# Проверка наличия файлов локально
check_files() {
    log_step "Проверка файлов"
    for file in "${CONFIG_FILES[@]}"; do
        if [[ ! -f "${SCRIPT_DIR}/${file}" ]]; then
            log_error "Файл не найден: ${SCRIPT_DIR}/${file}"
            exit 1
        fi
        log_info "  ✔ ${file}"
    done
}

# Проверка SSH доступности узла с машины
check_ssh_from_local() {
    local node=$1
    if ssh -o ConnectTimeout=5 -o BatchMode=yes "${node}" "exit" 2>/dev/null; then
        return 0
    else
        return 1
    fi
}

# Деплой конфигов
deploy_configs() {
    log_step "Копирование конфигов с локальной машины на узлы"

    FAILED_NODES=()

    for node in "${NODES[@]}"; do
        log_info "--- Узел: ${node} ---"

        # Проверяем SSH с локальной машины
        if ! check_ssh_from_local "${node}"; then
            log_error "  ✘ ${node} недоступен по SSH с локальной машины. Пропускаем."
            FAILED_NODES+=("${node}")
            continue
        fi

        # Создаём директорию если не существует
        ssh "${node}" "mkdir -p ${HADOOP_CONF_DIR}" || {
            log_warn "  Не удалось создать директорию на ${node}"
        }

        # Копируем каждый файл
        local node_ok=true
        for file in "${CONFIG_FILES[@]}"; do
            log_info "  scp ${file} → ${node}:${HADOOP_CONF_DIR}/"
            if scp "${SCRIPT_DIR}/${file}" "${node}:${HADOOP_CONF_DIR}/${file}"; then
                log_info "  ✔ ${file}"
            else
                log_error "  ✘ Ошибка копирования ${file} на ${node}"
                node_ok=false
            fi
        done

        if [[ "${node_ok}" == false ]]; then
            FAILED_NODES+=("${node}")
        fi
    done

    # Итог деплоя
    if [[ ${#FAILED_NODES[@]} -gt 0 ]]; then
        log_error "Деплой не удался на узлы: ${FAILED_NODES[*]}"
        exit 1
    fi

    log_info "✔ Конфиги успешно разложены на все узлы."
}

# Верификация файлов на узлах (с локальной машины)
verify_configs() {
    log_step "Верификация конфигов на узлах"

    for node in "${NODES[@]}"; do
        log_info "  ${node}:"
        for file in "${CONFIG_FILES[@]}"; do
            if ssh "${node}" "test -f ${HADOOP_CONF_DIR}/${file}" 2>/dev/null; then
                log_info "    ✔ ${HADOOP_CONF_DIR}/${file}"
            else
                log_error "    ✘ Файл отсутствует: ${HADOOP_CONF_DIR}/${file}"
            fi
        done
    done
}

# Заапуск YARN
restart_yarn() {
    log_step "Запуск YARN (запускается с nn)"

    if ! check_ssh_from_local "nn"; then
        log_error "nn недоступен. Запуск YARN пропущен."
        return 1
    fi

    # Определяем JAVA_HOME на удалённом узле автоматически
    REMOTE_JAVA_HOME=$(ssh nn "
        if [ -n \"\$JAVA_HOME\" ]; then
            echo \$JAVA_HOME
            exit 0
        fi
        echo ''
    ")

    log_info "Используем JAVA_HOME=${REMOTE_JAVA_HOME}"

    ssh nn "
        export JAVA_HOME=${REMOTE_JAVA_HOME}
        export HADOOP_HOME=${HADOOP_HOME}
        export PATH=\$JAVA_HOME/bin:\$HADOOP_HOME/bin:\$HADOOP_HOME/sbin:\$PATH

        HADOOP_ENV=${HADOOP_HOME}/etc/hadoop/hadoop-env.sh
        if ! grep -q 'export JAVA_HOME' \$HADOOP_ENV 2>/dev/null; then
            echo \"export JAVA_HOME=${REMOTE_JAVA_HOME}\" >> \$HADOOP_ENV
            echo '[YARN] JAVA_HOME добавлен в hadoop-env.sh'
        else
            sed -i \"s|^export JAVA_HOME=.*|export JAVA_HOME=${REMOTE_JAVA_HOME}|\" \$HADOOP_ENV
            echo '[YARN] JAVA_HOME обновлён в hadoop-env.sh'
        fi

        echo '[YARN] Запуск'
        start-yarn.sh
    "

    log_info "✔ YARN запущен."
}


# Запуск JobHistoryServer на nn
start_jobhistory() {
    log_step "Запуск JobHistoryServer на nn"

    if ! check_ssh_from_local "nn"; then
        log_error "nn недоступен. Запуск JobHistoryServer пропущен."
        return 1
    fi

    # Проверяем — запущен ли уже
    JHS_RUNNING=$(ssh nn "jps 2>/dev/null | grep -c 'JobHistoryServer'" 2>/dev/null | tr -d '[:space:]')
    [[ "${JHS_RUNNING}" =~ ^[0-9]+$ ]] || JHS_RUNNING=0

    if [[ "${JHS_RUNNING}" -gt 0 ]]; then
        log_warn "JobHistoryServer уже запущен. Перезапускаем..."
        ssh nn "
            export HADOOP_HOME=${HADOOP_HOME}
            export PATH=\$HADOOP_HOME/bin:\$HADOOP_HOME/sbin:\$PATH
            \$HADOOP_HOME/bin/mapred --daemon stop historyserver
            sleep 3
            \$HADOOP_HOME/bin/mapred --daemon start historyserver
        "
    else
        log_info "Запускаем JobHistoryServer..."
        ssh nn "
            export HADOOP_HOME=${HADOOP_HOME}
            export PATH=\$HADOOP_HOME/bin:\$HADOOP_HOME/sbin:\$PATH
            \$HADOOP_HOME/bin/mapred --daemon start historyserver
        "
    fi

    # Проверка запуска
    sleep 3
    JHS_CHECK=$(ssh nn "jps 2>/dev/null | grep -c 'JobHistoryServer'" || echo "0")
    if [[ "${JHS_CHECK}" -gt 0 ]]; then
        log_info "✔ JobHistoryServer успешно запущен."
        log_info "  Web UI: http://nn:19888"
    else
        log_error "✘ JobHistoryServer не запустился. Смотрите логи:"
        log_error "  ${HADOOP_HOME}/logs/mapred-*-historyserver-*.log"
        return 1
    fi
}

# Статус всех сервисов через jps на каждом узле
check_services_status() {
    log_step "Статус сервисов (jps)"

    for node in "${NODES[@]}"; do
        log_info "  --- ${node} ---"
        if check_ssh_from_local "${node}"; then
            # Подключаемся к каждому узлу напрямую с локальной машины
            ssh "${node}" "jps 2>/dev/null | sort" | while read -r line; do
                log_info "    ${line}"
            done
        else
            log_warn "    ${node} недоступен"
        fi
    done
}

main() {
    echo "================================================"
    echo "  Развертка конфигурации YARN"
    echo "  Узлы: ${NODES[*]}"
    echo "================================================"

    # 1. Создать конфиги
    create_config_files

    # 2. Проверить файлы
    check_files

    # 3. Разложить конфиги на все узлы напрямую с локальной машины
    deploy_configs

    # 4. Верификация
    verify_configs

    # 5. Запуск YARN
    echo ""
    read -r -p "Перезапустить YARN? (y/n): " answer
    if [[ "${answer}" =~ ^[Yy]$ ]]; then
        restart_yarn
    else
        log_warn "Пропущено. Вручную: ssh nn 'start-yarn.sh'"
    fi

    # 6. JobHistoryServer
    echo ""
    read -r -p "Запустить JobHistoryServer? (y/n): " answer
    if [[ "${answer}" =~ ^[Yy]$ ]]; then
        start_jobhistory
    else
        log_warn "Пропущено. Вручную: ssh nn 'mapred --daemon start historyserver'"
    fi

    # 7. Статус сервисов
    echo ""
    read -r -p "Показать статус сервисов (jps)? (y/n): " answer
    if [[ "${answer}" =~ ^[Yy]$ ]]; then
        check_services_status
    fi

    echo ""
    echo "================================================"
    log_info "✔ Готово!"
    echo "  YARN ResourceManager : http://nn:8088"
    echo "  JobHistoryServer     : http://nn:19888"
    echo "================================================"
}

main "$@"