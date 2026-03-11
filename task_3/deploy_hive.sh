#!/bin/bash
set -e

# =============================================================================
# КОНФИГУРАЦИЯ — измените перед запуском
# =============================================================================

# Версия Hive
HIVE_VERSION="4.0.0-alpha-2"
HIVE_ARCHIVE="apache-hive-${HIVE_VERSION}-bin.tar.gz"
HIVE_DIR="apache-hive-${HIVE_VERSION}-bin"
HIVE_URL="https://archive.apache.org/dist/hive/hive-${HIVE_VERSION}/${HIVE_ARCHIVE}"

# JDBC драйвер PostgreSQL
JDBC_VERSION="42.7.4"
JDBC_JAR="postgresql-${JDBC_VERSION}.jar"
JDBC_URL="https://jdbc.postgresql.org/download/${JDBC_JAR}"

# Узлы кластера
JN="jn"
NN="nn"
DN01="dn-01"

# IP адреса (для pg_hba.conf)
JN_IP=<JN IP>
NN_IP=<JN IP>
DN01_IP=<JN IP>

# Пользователи
HADOOP_USER="hadoop"
UBUNTU_USER="ubuntu"

# PostgreSQL
PG_VERSION="16"
PG_DB="metastore"
PG_USER="hive"
PG_PASSWORD="hive_password"
PG_PORT="5432"

# HiveServer2
HIVE_SERVER_PORT="5433"

# Пути
HADOOP_HOME_REMOTE="/home/hadoop"
HIVE_HOME_REMOTE="${HADOOP_HOME_REMOTE}/${HIVE_DIR}"

# =============================================================================
# Цвета и логирование
# =============================================================================

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

log_info()    { echo -e "${GREEN}[INFO]${NC}    $1"; }
log_warn()    { echo -e "${YELLOW}[WARN]${NC}    $1"; }
log_error()   { echo -e "${RED}[ERROR]${NC}   $1"; }
log_step()    { echo -e "\n${BLUE}╔══════════════════════════════════════╗${NC}"; \
                echo -e "${BLUE}║${NC} ${CYAN}$1${NC}"; \
                echo -e "${BLUE}╚══════════════════════════════════════╝${NC}"; }

# =============================================================================
# Вспомогательные функции
# =============================================================================

# Проверка SSH доступности узла
check_ssh() {
    local node=$1
    local user=$2
    if ssh -o ConnectTimeout=5 -o BatchMode=yes "${user}@${node}" "exit" 2>/dev/null; then
        return 0
    else
        return 1
    fi
}

# Выполнить команду на удалённом узле от нужного пользователя
run_remote() {
    local node=$1
    local user=$2
    local cmd=$3
    ssh "${user}@${node}" "${cmd}"
}

# Выполнить команду на узле через sudo от пользователя ubuntu
run_sudo() {
    local node=$1
    local cmd=$2
    ssh "${UBUNTU_USER}@${node}" "sudo ${cmd}"
}

# =============================================================================
# ШАГ 1 — Проверка SSH доступности всех узлов
# =============================================================================
check_all_nodes() {
    log_step "ШАГ 1: Проверка SSH доступности узлов"

    local all_ok=true

    for node in "${JN}" "${NN}" "${DN01}"; do
        if check_ssh "${node}" "${UBUNTU_USER}"; then
            log_info "✔ ${node} (${UBUNTU_USER}) — доступен"
        else
            log_error "✘ ${node} (${UBUNTU_USER}) — недоступен"
            all_ok=false
        fi
    done

    # Проверяем доступ под hadoop на jn и nn
    for node in "${JN}" "${NN}"; do
        if check_ssh "${node}" "${HADOOP_USER}"; then
            log_info "✔ ${node} (${HADOOP_USER}) — доступен"
        else
            log_error "✘ ${node} (${HADOOP_USER}) — недоступен"
            all_ok=false
        fi
    done

    if [[ "${all_ok}" == false ]]; then
        log_error "Не все узлы доступны. Прерываем выполнение."
        exit 1
    fi

    log_info "✔ Все узлы доступны."
}

# =============================================================================
# ШАГ 2 — Скачивание и доставка дистрибутива Hive на jn
# =============================================================================
download_and_upload_hive() {
    log_step "ШАГ 2: Скачивание и доставка Hive на ${JN}"

    # Скачиваем локально если ещё нет
    if [[ ! -f "${HIVE_ARCHIVE}" ]]; then
        log_info "Скачиваем ${HIVE_ARCHIVE}..."
        wget -q --show-progress "${HIVE_URL}"
    else
        log_warn "Архив ${HIVE_ARCHIVE} уже существует локально, пропускаем скачивание."
    fi

    # Проверяем — есть ли уже архив на jn
    ARCHIVE_EXISTS=$(run_remote "${JN}" "${HADOOP_USER}" \
        "test -f ${HADOOP_HOME_REMOTE}/${HIVE_ARCHIVE} && echo yes || echo no")

    if [[ "${ARCHIVE_EXISTS}" == "no" ]]; then
        log_info "Копируем архив на ${JN}..."
        scp "${HIVE_ARCHIVE}" "${HADOOP_USER}@${JN}:${HADOOP_HOME_REMOTE}/"
        log_info "✔ Архив скопирован на ${JN}."
    else
        log_warn "Архив уже есть на ${JN}, пропускаем копирование."
    fi

    # Распаковываем на jn если каталог ещё не существует
    DIR_EXISTS=$(run_remote "${JN}" "${HADOOP_USER}" \
        "test -d ${HIVE_HOME_REMOTE} && echo yes || echo no")

    if [[ "${DIR_EXISTS}" == "no" ]]; then
        log_info "Распаковываем архив на ${JN}..."
        run_remote "${JN}" "${HADOOP_USER}" \
            "cd ${HADOOP_HOME_REMOTE} && tar -xzvf ${HIVE_ARCHIVE}"
        log_info "✔ Архив распакован."
    else
        log_warn "Каталог ${HIVE_DIR} уже существует на ${JN}, пропускаем распаковку."
    fi

    # Скачиваем JDBC драйвер в bin/ Hive
    JDBC_EXISTS=$(run_remote "${JN}" "${HADOOP_USER}" \
        "test -f ${HIVE_HOME_REMOTE}/lib/${JDBC_JAR} && echo yes || echo no")

    if [[ "${JDBC_EXISTS}" == "no" ]]; then
        log_info "Скачиваем JDBC драйвер PostgreSQL на ${JN}..."
        run_remote "${JN}" "${HADOOP_USER}" \
            "cd ${HIVE_HOME_REMOTE}/lib && wget -q ${JDBC_URL}"
        log_info "✔ JDBC драйвер скачан."
    else
        log_warn "JDBC драйвер уже существует, пропускаем."
    fi
}

# =============================================================================
# ШАГ 3 — Установка и настройка PostgreSQL на dn-01
# =============================================================================
setup_postgresql() {
    log_step "ШАГ 3: Установка и настройка PostgreSQL на ${DN01}"

    # 3.1 Установка PostgreSQL
    log_info "Устанавливаем PostgreSQL на ${DN01}..."
    run_sudo "${DN01}" "apt-get install -y postgresql"
    log_info "✔ PostgreSQL установлен."

    # 3.2 Создание БД и пользователя
    log_info "Создаём базу данных и пользователя..."
    run_sudo "${DN01}" \
        "sudo -i -u postgres psql -c \"CREATE DATABASE ${PG_DB};\" 2>/dev/null || true"
    run_sudo "${DN01}" \
        "sudo -i -u postgres psql -c \"CREATE USER ${PG_USER} WITH PASSWORD '${PG_PASSWORD}';\" 2>/dev/null || true"
    run_sudo "${DN01}" \
        "sudo -i -u postgres psql -c \"GRANT ALL PRIVILEGES ON DATABASE ${PG_DB} TO ${PG_USER};\""
    run_sudo "${DN01}" \
        "sudo -i -u postgres psql -c \"ALTER DATABASE ${PG_DB} OWNER TO ${PG_USER};\""
    log_info "✔ База данных и пользователь созданы."

    # 3.3 Настройка listen_addresses
    log_info "Настраиваем listen_addresses в postgresql.conf..."
    PG_CONF="/etc/postgresql/${PG_VERSION}/main/postgresql.conf"
    run_sudo "${DN01}" \
        "sed -i \"s|^#listen_addresses.*|listen_addresses = '${DN01}'|\" ${PG_CONF}"
    log_info "✔ listen_addresses настроен."

    # 3.4 Настройка pg_hba.conf
    log_info "Настраиваем pg_hba.conf..."
    PG_HBA="/etc/postgresql/${PG_VERSION}/main/pg_hba.conf"

    # Комментируем стандартную строку IPv4 и добавляем наши правила
    run_sudo "${DN01}" "bash -c \"
        sed -i 's|^host.*all.*all.*127.0.0.1/32.*|#&|' ${PG_HBA}
        echo 'host    ${PG_DB}    ${PG_USER}    ${DN01_IP}/32    password' >> ${PG_HBA}
        echo 'host    ${PG_DB}    ${PG_USER}    ${NN_IP}/32      password' >> ${PG_HBA}
        echo 'host    ${PG_DB}    ${PG_USER}    ${JN_IP}/32      password' >> ${PG_HBA}
    \""
    log_info "✔ pg_hba.conf настроен."

    # 3.5 Перезапуск PostgreSQL
    log_info "Перезапускаем PostgreSQL..."
    run_sudo "${DN01}" "systemctl restart postgresql"
    sleep 3
    log_info "✔ PostgreSQL перезапущен."
}

# =============================================================================
# ШАГ 4 — Установка PostgreSQL клиента на jn и nn
# =============================================================================
install_pg_client() {
    log_step "ШАГ 4: Установка PostgreSQL-клиента на ${JN} и ${NN}"

    for node in "${JN}" "${NN}"; do
        log_info "Устанавливаем postgresql-client на ${node}..."
        run_sudo "${node}" "apt-get install -y postgresql-client-${PG_VERSION}"
        log_info "✔ postgresql-client установлен на ${node}."
    done

    # Проверка подключения с jn
    log_info "Проверяем подключение к PostgreSQL с ${JN}..."
    if run_remote "${JN}" "${UBUNTU_USER}" \
        "PGPASSWORD=${PG_PASSWORD} psql -h ${DN01} -p ${PG_PORT} \
         -U ${PG_USER} -d ${PG_DB} -c '\q'" 2>/dev/null; then
        log_info "✔ Подключение к PostgreSQL с ${JN} успешно."
    else
        log_error "✘ Не удалось подключиться к PostgreSQL с ${JN}."
        exit 1
    fi

    # Проверка подключения с nn
    log_info "Проверяем подключение к PostgreSQL с ${NN}..."
    if run_remote "${NN}" "${UBUNTU_USER}" \
        "PGPASSWORD=${PG_PASSWORD} psql -h ${DN01} -p ${PG_PORT} \
         -U ${PG_USER} -d ${PG_DB} -c '\q'" 2>/dev/null; then
        log_info "✔ Подключение к PostgreSQL с ${NN} успешно."
    else
        log_error "✘ Не удалось подключиться к PostgreSQL с ${NN}."
        exit 1
    fi
}

# =============================================================================
# ШАГ 5 — Создание hive-site.xml и настройка окружения на jn
# =============================================================================
configure_hive_on_jn() {
    log_step "ШАГ 5: Настройка Hive на ${JN}"

    # 5.1 Создаём hive-site.xml
    log_info "Создаём hive-site.xml на ${JN}..."
    run_remote "${JN}" "${HADOOP_USER}" "cat > ${HIVE_HOME_REMOTE}/conf/hive-site.xml << 'EOF'
<configuration>

    <!-- Аутентификация -->
    <property>
        <name>hive.server2.authentication</name>
        <value>NONE</value>
    </property>

    <!-- Директория warehouse в HDFS -->
    <property>
        <name>hive.metastore.warehouse.dir</name>
        <value>/user/hive/warehouse</value>
    </property>

    <!-- Порт HiveServer2 -->
    <property>
        <name>hive.server2.thrift.port</name>
        <value>${HIVE_SERVER_PORT}</value>
    </property>

    <!-- Подключение к PostgreSQL metastore -->
    <property>
        <name>javax.jdo.option.ConnectionURL</name>
        <value>jdbc:postgresql://${DN01}:${PG_PORT}/${PG_DB}</value>
    </property>
    <property>
        <name>javax.jdo.option.ConnectionDriverName</name>
        <value>org.postgresql.Driver</value>
    </property>
    <property>
        <name>javax.jdo.option.ConnectionUserName</name>
        <value>${PG_USER}</value>
    </property>
    <property>
        <name>javax.jdo.option.ConnectionPassword</name>
        <value>${PG_PASSWORD}</value>
    </property>

</configuration>
EOF"
    log_info "✔ hive-site.xml создан."

    # 5.2 Настройка .profile на jn
    log_info "Настраиваем переменные окружения на ${JN}..."
    run_remote "${JN}" "${HADOOP_USER}" "bash -c \"
        # Удаляем старые записи если есть
        sed -i '/HIVE_HOME/d' ~/.profile
        sed -i '/HIVE_CONF_DIR/d' ~/.profile
        sed -i '/HIVE_AUX_JARS_PATH/d' ~/.profile

        # Добавляем свежие
        echo 'export HIVE_HOME=${HIVE_HOME_REMOTE}'          >> ~/.profile
        echo 'export HIVE_CONF_DIR=\$HIVE_HOME/conf'         >> ~/.profile
        echo 'export HIVE_AUX_JARS_PATH=\$HIVE_HOME/lib/*'   >> ~/.profile
        echo 'export PATH=\$PATH:\$HIVE_HOME/bin'             >> ~/.profile
    \""
    log_info "✔ Переменные окружения настроены на ${JN}."

    # Проверка версии Hive
    log_info "Проверяем версию Hive на ${JN}..."
    run_remote "${JN}" "${HADOOP_USER}" \
        "source ~/.profile && hive --version 2>/dev/null | head -1"
}

# =============================================================================
# ШАГ 6 — Копирование Hive на nn и настройка окружения
# =============================================================================
deploy_hive_to_nn() {
    log_step "ШАГ 6: Копирование Hive на ${NN}"

    DIR_EXISTS_NN=$(run_remote "${NN}" "${HADOOP_USER}" \
        "test -d ${HIVE_HOME_REMOTE} && echo yes || echo no")

    if [[ "${DIR_EXISTS_NN}" == "no" ]]; then
        log_info "Копируем Hive с ${JN} на ${NN}..."
        run_remote "${JN}" "${HADOOP_USER}" \
            "scp -r ${HIVE_HOME_REMOTE} ${HADOOP_USER}@${NN}:${HADOOP_HOME_REMOTE}/"
        log_info "✔ Hive скопирован на ${NN}."
    else
        log_warn "Hive уже установлен на ${NN}, пропускаем."
    fi

    # Настройка .profile на nn
    log_info "Настраиваем переменные окружения на ${NN}..."
    run_remote "${NN}" "${HADOOP_USER}" "bash -c \"
        sed -i '/HIVE_HOME/d' ~/.profile
        sed -i '/HIVE_CONF_DIR/d' ~/.profile
        sed -i '/HIVE_AUX_JARS_PATH/d' ~/.profile

        echo 'export HIVE_HOME=${HIVE_HOME_REMOTE}'          >> ~/.profile
        echo 'export HIVE_CONF_DIR=\$HIVE_HOME/conf'         >> ~/.profile
        echo 'export HIVE_AUX_JARS_PATH=\$HIVE_HOME/lib/*'   >> ~/.profile
        echo 'export PATH=\$PATH:\$HIVE_HOME/bin'             >> ~/.profile
    \""
    log_info "✔ Переменные окружения настроены на ${NN}."
}

# =============================================================================
# ШАГ 7 — Инициализация схемы метастора
# =============================================================================
init_schema() {
    log_step "ШАГ 7: Инициализация схемы метастора"

    # Создаём директории в HDFS
    log_info "Создаём директории в HDFS..."
    run_remote "${JN}" "${HADOOP_USER}" "
        source ~/.profile
        hdfs dfs -mkdir -p /user/hive/warehouse
        hdfs dfs -chmod g+w /tmp
        hdfs dfs -chmod g+w /user/hive/warehouse
    "
    log_info "✔ Директории HDFS созданы."

    # Инициализируем схему
    log_info "Инициализируем схему БД (schematool)..."
    run_remote "${JN}" "${HADOOP_USER}" "
        source ~/.profile
        cd ${HIVE_HOME_REMOTE}
        bin/schematool -dbType postgres -initSchema
    "
    log_info "✔ Схема метастора инициализирована."
}

# =============================================================================
# ШАГ 8 — Запуск HiveServer2
# =============================================================================
start_hiveserver2() {
    log_step "ШАГ 8: Запуск HiveServer2 на ${JN}"

    # Проверяем — не запущен ли уже
    HS2_RUNNING=$(run_remote "${JN}" "${HADOOP_USER}" \
        "jps 2>/dev/null | grep -c 'RunJar'" | tr -d '[:space:]')
    [[ "${HS2_RUNNING}" =~ ^[0-9]+$ ]] || HS2_RUNNING=0

    if [[ "${HS2_RUNNING}" -gt 0 ]]; then
        log_warn "HiveServer2 уже запущен."
    else
        log_info "Запускаем HiveServer2..."
        run_remote "${JN}" "${HADOOP_USER}" "
            source ~/.profile
            nohup hive \
                --hiveconf hive.server2.enable.doAs=false \
                --hiveconf hive.security.authorization.enabled=false \
                --service hiveserver2 \
                1>> /tmp/hs2.log \
                2>> /tmp/hs2e.log &
            echo \$! > /tmp/hs2.pid
        "

        # Ждём запуска
        log_info "Ожидаем запуска HiveServer2 (30 сек)..."
        sleep 30

        # Проверяем
        HS2_CHECK=$(run_remote "${JN}" "${HADOOP_USER}" \
            "jps 2>/dev/null | grep -c 'RunJar'" | tr -d '[:space:]')
        [[ "${HS2_CHECK}" =~ ^[0-9]+$ ]] || HS2_CHECK=0

        if [[ "${HS2_CHECK}" -gt 0 ]]; then
            log_info "✔ HiveServer2 запущен."
            log_info "  Логи: /tmp/hs2.log, /tmp/hs2e.log"
            log_info "  Web UI: http://${JN}:10002"
        else
            log_error "✘ HiveServer2 не запустился. Смотрите логи:"
            run_remote "${JN}" "${HADOOP_USER}" "tail -20 /tmp/hs2e.log"
            exit 1
        fi
    fi
}

# =============================================================================
# ШАГ 9 — Загрузка данных в партиционированную таблицу
# =============================================================================
load_partitioned_data() {
    log_step "ШАГ 9: Загрузка данных в партиционированную таблицу"

    # 9.1 Создаём тестовый Parquet-файл через Python на jn
    log_info "Создаём тестовый Parquet-файл на ${JN}..."
    run_remote "${JN}" "${HADOOP_USER}" "python3 -c \"
import pandas as pd

df = pd.DataFrame({
    'id':        [1, 2, 3, 4, 5, 6],
    'sale_date': ['2024-01-15', '2024-01-20', '2024-02-05',
                  '2024-02-18', '2024-03-10', '2024-03-22'],
    'product':   ['laptop', 'phone', 'desk', 'chair', 'tablet', 'lamp'],
    'amount':    [1200.00, 800.00, 350.00, 150.00, 600.00, 45.00],
    'category':  ['electronics', 'electronics', 'furniture',
                  'furniture', 'electronics', 'furniture']
})

df.to_parquet('/home/hadoop/sales.parquet', index=False)
print('Parquet файл создан')
\""
    log_info "✔ Parquet-файл создан."

    # 9.2 Загружаем файл в HDFS
    log_info "Загружаем Parquet-файл в HDFS..."
    run_remote "${JN}" "${HADOOP_USER}" "
        source ~/.profile
        hdfs dfs -mkdir -p /user/hadoop/sales_raw
        hdfs dfs -put -f /home/hadoop/sales.parquet /user/hadoop/sales_raw/
    "
    log_info "✔ Файл загружен в HDFS."

    # 9.3 Создаём таблицы и загружаем данные через Beeline
    log_info "Создаём таблицы и загружаем данные через Beeline..."
    run_remote "${JN}" "${HADOOP_USER}" "
        source ~/.profile

        beeline -u jdbc:hive2://${JN}:${HIVE_SERVER_PORT} \
            -n scott -p tiger \
            --silent=true \
            -e \"
                CREATE DATABASE IF NOT EXISTS shop;

                USE shop;

                -- Внешняя staging-таблица поверх Parquet
                CREATE EXTERNAL TABLE IF NOT EXISTS sales_staging (
                    id        INT,
                    sale_date STRING,
                    product   STRING,
                    amount    DOUBLE,
                    category  STRING
                )
                STORED AS PARQUET
                LOCATION '/user/hadoop/sales_raw/';

                -- Партиционированная таблица
                CREATE TABLE IF NOT EXISTS sales_partitioned (
                    id        INT,
                    sale_date STRING,
                    product   STRING,
                    amount    DOUBLE
                )
                PARTITIONED BY (
                    category STRING,
                    year     STRING,
                    month    STRING
                )
                STORED AS PARQUET;

                -- Включаем динамическое партиционирование
                SET hive.exec.dynamic.partition = true;
                SET hive.exec.dynamic.partition.mode = nonstrict;

                -- Загружаем данные
                INSERT INTO TABLE sales_partitioned
                PARTITION (category, year, month)
                SELECT
                    id,
                    sale_date,
                    product,
                    amount,
                    category,
                    YEAR(sale_date)                AS year,
                    LPAD(MONTH(sale_date), 2, '0') AS month
                FROM sales_staging;

                -- Проверяем партиции
                SHOW PARTITIONS sales_partitioned;

                -- Проверяем данные
                SELECT category, year, month, COUNT(*) AS cnt, SUM(amount) AS revenue
                FROM sales_partitioned
                GROUP BY category, year, month
                ORDER BY year, month, category;
            \"
    "
    log_info "✔ Данные загружены в партиционированную таблицу."

    # 9.4 Проверяем структуру в HDFS
    log_info "Структура партиций в HDFS:"
    run_remote "${JN}" "${HADOOP_USER}" "
        source ~/.profile
        hdfs dfs -ls -R /user/hive/warehouse/shop.db/sales_partitioned/
    "
}

# =============================================================================
# Итоговый статус
# =============================================================================
print_summary() {
    echo ""
    echo -e "${GREEN}╔══════════════════════════════════════════════╗${NC}"
    echo -e "${GREEN}║           Развёртывание завершено!           ║${NC}"
    echo -e "${GREEN}╚══════════════════════════════════════════════╝${NC}"
    echo ""
    echo -e "  ${CYAN}Сервисы:${NC}"
    echo -e "  HiveServer2 Web UI  : http://${JN}:10002"
    echo -e "  Beeline             : beeline -u jdbc:hive2://${JN}:${HIVE_SERVER_PORT}"
    echo ""
    echo -e "  ${CYAN}Логи HiveServer2:${NC}"
    echo -e "  ssh ${HADOOP_USER}@${JN} 'tail -f /tmp/hs2.log'"
    echo -e "  ssh ${HADOOP_USER}@${JN} 'tail -f /tmp/hs2e.log'"
    echo ""
    echo -e "  ${CYAN}Проброс портов:${NC}"
    echo -e "  ssh -L 9870:${NN_IP}:9870 \\"
    echo -e "      -L 8088:${NN_IP}:8088 \\"
    echo -e "      -L 19888:${NN_IP}:19888 \\"
    echo -e "      -L 10002:${JN_IP}:10002 \\"
    echo -e "      ${UBUNTU_USER}@<внешний-ip>"
    echo ""
}

# =============================================================================
# MAIN
# =============================================================================
main() {

    check_all_nodes        # Шаг 1: проверка доступности узлов
    download_and_upload_hive  # Шаг 2: скачивание и доставка Hive
    setup_postgresql       # Шаг 3: установка и настройка PostgreSQL
    install_pg_client      # Шаг 4: установка pg-клиента и проверка подключения
    configure_hive_on_jn   # Шаг 5: конфигурация Hive на jn
    deploy_hive_to_nn      # Шаг 6: копирование Hive на nn
    init_schema            # Шаг 7: инициализация схемы метастора
    start_hiveserver2      # Шаг 8: запуск HiveServer2
    load_partitioned_data  # Шаг 9: загрузка данных в партиционированную таблицу

    print_summary
}

main "$@"
