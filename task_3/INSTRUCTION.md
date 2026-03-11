# Развёртывание Apache Hive 4.0.0-alpha-2


## 1. Установка Hive

Все действия выполняются от пользователя `hadoop` на **Jump Node (jn)**.

### 1.1. Скачивание дистрибутива

```bash
# На локальной машине — скачиваем дистрибутив
wget https://archive.apache.org/dist/hive/hive-4.0.0-alpha-2/apache-hive-4.0.0-alpha-2-bin.tar.gz

# Копируем на Jump Node
scp apache-hive-4.0.0-alpha-2-bin.tar.gz jn:/home/hadoop/
```

### 1.2. Распаковка архива

```bash
# На jn, пользователь hadoop
tar -xzvf apache-hive-4.0.0-alpha-2-bin.tar.gz
```

### 1.3. Скачивание JDBC-драйвера PostgreSQL

```bash
cd apache-hive-4.0.0-alpha-2-bin/bin
wget https://jdbc.postgresql.org/download/postgresql-42.7.4.jar
```

---

## 2. Установка и настройка PostgreSQL

Все действия выполняются на **dn-01** от пользователя `ubuntu`.

### 2.1. Установка PostgreSQL

```bash
sudo apt install postgresql
```

### 2.2. Создание базы данных и пользователя

```bash
# Переключаемся на пользователя postgres
sudo -i -u postgres
psql
```

```sql
-- Создаём базу данных для метаданных Hive
CREATE DATABASE metastore;

-- Создаём пользователя hive
CREATE USER hive WITH PASSWORD '<пароль для пользователя hive>';

-- Выдаём права на базу данных
GRANT ALL PRIVILEGES ON DATABASE metastore TO hive;

-- Назначаем владельца
ALTER DATABASE metastore OWNER TO hive;

-- Выходим
\q
```

### 2.3. Настройка адреса прослушивания

```bash
sudo nano /etc/postgresql/16/main/postgresql.conf
```

Находим и меняем строку:

```ini
# Было:
#listen_addresses = 'localhost'

# Стало:
listen_addresses = 'dn-01'
```

### 2.4. Настройка прав подключения

```bash
sudo nano /etc/postgresql/16/main/pg_hba.conf
```

Находим секцию `# IPv4 local connections` и заменяем на:

```ini
# IPv4 local connections:
# Разрешаем подключение только пользователю hive к базе metastore
# с конкретных узлов кластера
host    metastore    hive    <адрес dn-01>/32    password
host    metastore    hive    <адрес nn>/32    password
```

### 2.5. Перезапуск PostgreSQL

```bash
sudo systemctl restart postgresql
```

### 2.6. Проверка подключения

Устанавливаем PostgreSQL-клиент на **jn** и **nn**:

```bash
# На jn и nn (пользователь ubuntu)
sudo apt install postgresql-client-16
```

Проверяем подключение с обоих узлов:

```bash
psql -h dn-01 -p 5432 -U hive -W -d metastore
```

Если подключение прошло успешно — конфигурация PostgreSQL выполнена верно.

---

## 3. Настройка Hive

Все действия выполняются на **jn** от пользователя `hadoop`.

### 3.1. Создание hive-site.xml

```bash
cd ~/apache-hive-4.0.0-alpha-2-bin/conf
nano hive-site.xml
```

```xml
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
        <value>5433</value>
        <description>TCP port number to listen on, default 10000</description>
    </property>

    <!-- Подключение к PostgreSQL metastore -->
    <property>
        <name>javax.jdo.option.ConnectionURL</name>
        <value>jdbc:postgresql://dn-01:5432/metastore</value>
    </property>
    <property>
        <name>javax.jdo.option.ConnectionDriverName</name>
        <value>org.postgresql.Driver</value>
    </property>
    <property>
        <name>javax.jdo.option.ConnectionUserName</name>
        <value>hive</value>
    </property>
    <property>
        <name>javax.jdo.option.ConnectionPassword</name>
        <value><пароль для пользователя hive></value>
    </property>

</configuration>
```

### 3.2. Настройка переменных окружения

```bash
cd ~
nano .profile
```

Добавляем в конец файла:

```bash
export HIVE_HOME=/home/hadoop/apache-hive-4.0.0-alpha-2-bin
export HIVE_CONF_DIR=$HIVE_HOME/conf
export HIVE_AUX_JARS_PATH=$HIVE_HOME/lib/*
export PATH=$PATH:$HIVE_HOME/bin
```

Применяем изменения:

```bash
source .profile
```

Проверяем:

```bash
hive --version
```

### 3.3. Копирование Hive на Name Node

```bash
scp -r ~/apache-hive-4.0.0-alpha-2-bin nn:/home/hadoop/
```

> После копирования необходимо также добавить переменные окружения из п. 3.2 в `.profile` на **nn**.

---

## 4. Инициализация схемы метастора

Выполняется на **jn** от пользователя `hadoop`.

```bash
cd ~/apache-hive-4.0.0-alpha-2-bin

# Создаём нужные директории в HDFS
hdfs dfs -mkdir -p /user/hive/warehouse
hdfs dfs -chmod g+w /tmp
hdfs dfs -chmod g+w /user/hive/warehouse

# Инициализируем схему базы данных
bin/schematool -dbType postgres -initSchema
```

Успешный вывод будет содержать:

```
Initialization script completed
schemaTool completed
```

---

## 5. Запуск HiveServer2

Выполняется на **jn** от пользователя `hadoop`.

```bash
hive \
  --hiveconf hive.server2.enable.doAs=false \
  --hiveconf hive.security.authorization.enabled=false \
  --service hiveserver2 \
  1>> /tmp/hs2.log \
  2>> /tmp/hs2e.log &
```

Проверяем логи запуска:

```bash
tail -f /tmp/hs2.log
tail -f /tmp/hs2e.log
```

---

## 6. Подключение через Beeline

```bash
beeline -u jdbc:hive2://jn:5433 -n scott -p tiger
```

---

## 7. Проброс портов

Для доступа к Web UI через браузер с локальной машины:

```bash
ssh -L 9870:192.168.10.31:9870 \   # HDFS NameNode UI
    -L 8088:192.168.10.31:8088 \   # YARN ResourceManager UI
    -L 19888:192.168.10.31:19888 \ # JobHistory Server UI
    -L 10002:192.168.10.22:10002 \ # HiveServer2 Web UI
    ubuntu@<внешний-ip>
```

Добавь альтернативный вариант раздела 8 — загрузка из Parquet-файла:

---

## 8. Загрузка данных в партиционированную таблицу из Parquet

### 8.1. Подготовка Parquet-файла

Parquet-файл создаём локально на **jn** с помощью Python:

```bash
# Устанавливаем необходимые библиотеки
pip install pandas pyarrow
```

```python
# Запускаем python3 и создаём тестовый датасет
python3
```

```python
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
print(df)
```

Загружаем Parquet-файл в HDFS:

```bash
hdfs dfs -mkdir -p /user/hadoop/sales_raw
hdfs dfs -put /home/hadoop/sales.parquet /user/hadoop/sales_raw/

# Проверяем
hdfs dfs -ls /user/hadoop/sales_raw/
```

---

### 8.2. Подключение к Hive через Beeline

```bash
beeline -u jdbc:hive2://jn:5433 -n scott -p tiger
```

---

### 8.3. Создание внешней staging-таблицы поверх Parquet

Поскольку файл уже в формате Parquet, создаём внешнюю таблицу
прямо поверх файла в HDFS — без копирования данных:

```sql
CREATE DATABASE IF NOT EXISTS shop;

USE shop;

CREATE EXTERNAL TABLE IF NOT EXISTS sales_staging (
    id        INT,
    sale_date STRING,
    product   STRING,
    amount    DOUBLE,
    category  STRING
)
STORED AS PARQUET
LOCATION '/user/hadoop/sales_raw/';
```

Проверяем что данные читаются:

```sql
SELECT * FROM sales_staging;
```

```
+----+------------+---------+---------+-------------+
| id | sale_date  | product | amount  |  category   |
+----+------------+---------+---------+-------------+
| 1  | 2024-01-15 | laptop  | 1200.0  | electronics |
| 2  | 2024-01-20 | phone   | 800.0   | electronics |
| 3  | 2024-02-05 | desk    | 350.0   | furniture   |
...
+----+------------+---------+---------+-------------+
```

### 8.4. Создание партиционированной таблицы

```sql
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
```

---

### 8.5. Включение динамического партиционирования

```sql
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
```

---

### 8.6. Загрузка данных из staging в партиционированную таблицу

```sql
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
```

---

### 8.7. Проверка результата

**Просмотр созданных партиций:**

```sql
SHOW PARTITIONS sales_partitioned;
```

```
+-------------------------------------------+
|               partition                   |
+-------------------------------------------+
| category=electronics/year=2024/month=01   |
| category=electronics/year=2024/month=02   |
| category=electronics/year=2024/month=03   |
| category=furniture/year=2024/month=02     |
| category=furniture/year=2024/month=03     |
+-------------------------------------------+
```
