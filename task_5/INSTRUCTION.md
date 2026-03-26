# Оркестрация обработки данных с Apache Spark на YARN с использованием Prefect

## Реализация

### Описание скрипта `script.py`

Скрипт реализует ETL-пайплайн из пяти последовательных задач, оркестрированных через Prefect:

```
start_spark → extract → transform → load → stop_spark
```

---

#### Задача 1: `start_spark` — запуск Spark-сессии

```python
@task(cache_policy=NO_CACHE)
def start_spark():
    spark = (
        SparkSession.builder
        .master("yarn")                      # запуск на кластере YARN
        .appName("sales_partitioned_etl")    # имя приложения в YARN UI
        .config("spark.sql.warehouse.dir", HIVE_WAREHOUSE)
        .config("hive.metastore.uris", HIVE_METASTORE)
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .enableHiveSupport()                 # поддержка Hive SQL и таблиц
        .getOrCreate()
    )
```

- Создаёт Spark-сессию под управлением **YARN**
- Подключается к **Hive Metastore** для работы с таблицами
- `NO_CACHE` — результат задачи не кэшируется, каждый запуск выполняется заново
- `dynamic` — позволяет перезаписывать только изменённые партиции

---

#### Задача 2: `extract` — чтение данных из HDFS

```python
@task(cache_policy=NO_CACHE)
def extract(spark):
    schema = StructType([
        StructField("id",        IntegerType(), True),
        StructField("sale_date", StringType(),  True),
        StructField("product",   StringType(),  True),
        StructField("amount",    DoubleType(),  True),
    ])

    df = (
        spark.read
        .option("basePath", HDFS_INPUT_PATH)
        .schema(schema)
        .parquet(f"{HDFS_INPUT_PATH}/category=*/year=*/month=*/")
    )
```

- Читает **Parquet-файлы** из всех партиций через wildcard `category=*/year=*/month=*/`

Результирующий DataFrame содержит колонки:

```
id | sale_date | product | amount | category | year | month
```

---

#### Задача 3: `transform` — трансформации данных

```python
@task(cache_policy=NO_CACHE)
def transform(df):
    df = (
        df
        .withColumn("sale_date",       F.to_date(F.col("sale_date"), "yyyy-MM-dd"))
        .withColumn("product",         F.initcap(F.col("product")))
        .withColumn("price_category",  F.when(F.col("amount") >= 1000, "premium")
                                        .when(F.col("amount") >= 300,  "mid")
                                        .otherwise("budget"))
        .withColumn("amount",          F.round(F.col("amount"), 2))
    )
```

Применяются четыре преобразования:
- Изменения типа данных у колонки sale_date
- Название продукта с заглавной буквы
- Разделение на ценовые категории в зависимости от цены
- Округление до 2 знаков

---

#### Задача 4: `load` — сохранение результатов в Hive

Создаёт базу данных `prefect_demo` и три таблицы:

**Таблица 1: `sales_transformed`** — полные трансформированные данные

**Таблица 2: `sales_by_category`** — агрегация по категории

**Таблица 3: `sales_by_month`** — агрегация по году, месяцу и категории
---

#### Задача 5: `stop_spark` — остановка сессии

---

#### Flow: `process_data` — оркестрация

```python
@flow(name="sales_partitioned_etl")
def process_data():
    spark = start_spark()   # 1. Запуск Spark на YARN
    df    = extract(spark)  # 2. Чтение данных из HDFS
    df    = transform(df)   # 3. Трансформации
    load(spark, df)         # 4. Сохранение таблиц в Hive
    stop_spark(spark)       # 5. Остановка Spark
```

Задачи выполняются **строго последовательно** — каждая ждёт результат предыдущей.

---

## Запуск

### Ручной запуск

```bash
# Подключаемся к nn
ssh hadoop@nn

# Активируем виртуальное окружение
source ~/hw_5/.venv/bin/activate

#Устанавливаем библиотеки
pip install prefect prefect-shell pyspark

# Запускаем скрипт
python3 ~/hw_5/script.py
```

### Автоматический запуск через sh-скрипт

```bash
# Подключаемся к jn
ssh hadoop@jn

chmod +x script.sh
./run_pipeline.sh
```