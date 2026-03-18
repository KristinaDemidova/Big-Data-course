## Домашнее задание №4

### Скачиваем spark

Заходим под юзером hadoop на jn
```
wget https://archive.apache.org/dist/spark/spark-3.5.3/spark-3.5.3-bin-hadoop3.tgz
tar -xzf spark-3.5.3-bin-hadoop3.tgz
```

### Затем в .profile добавляем новые переменные
```
export SPARK_DIST_CLASSPATH="/home/hadoop/spark-3.5.3-bin-hadoop3/jars/*:/home/hadoop/hadoop-3.4.1/etc/hadoop:/home/hadoop/hadoop-3.4.1/share/hadoop/common/lib/*:/home/hadoop/hadoop-3.4.1/share/hadoop/common/*:/home/hadoop/hadoop-3.4.1/share/hadoop/hdfs:/home/hadoop/hadoop-3.4.1/share/hadoop/hdfs/lib/*:/home/hadoop/hadoop-3.4.1/share/hadoop/hdfs/*:/home/hadoop/hadoop-3.4.1/share/hadoop/mapreduce/*:/home/hadoop/hadoop-3.4.1/share/hadoop/yarn:/home/hadoop/hadoop-3.4.1/share/hadoop/yarn/lib/*:/home/hadoop/hadoop-3.4.1/share/hadoop/yarn/*:/home/hadoop/apache-hive-4.0.0-alpha-2-bin/*:/home/hadoop/apache-hive-4.0.0-alpha-2-bin/lib/*"
export SPARK_HOME="/home/hadoop/spark-3.5.3-bin-hadoop3/"
```

### Копируем .profile на nn
```
scp .profile nn:/home/hadoop/
```

### Запускаем hive metastore

Заходим на nn
```
ssh nn
```
И запускаем
```
hive --hiveconf hive.server2.enable.doAs=false --hiveconf hive.security.authorization.enabled=false --service metastore 1>> /tmp/hms.log 2>> /tmp/hms.log &
```
Возвращаемся обратно на jn
```
exit
```

### Создаем виртуальное окружение и скачиваем нужные библиотеки

```
python3 -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install onetl
pip install ipython
pip install pyspark==3.5.3
```

На случай если нет пакета python3-venv, то нужно зайти под юзером ubuntu, поставить его
```
sudo apt install python3.12-venv
```

### ipython

Заходим в ipython и выполняем
```
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from onetl.connection import Hive
from onetl.db import DBWriter

spark = (
    SparkSession.builder.master("yarn")
    .appName("spark_check_yarn")
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
    .config("spark.hive.metastore.uris", "thrift://nn:9083")
    .enableHiveSupport()
    .getOrCreate()
)

hive = Hive(spark=spark, cluster="x")
hive.check()

# 1. Создаём данные
data = [
    (1, "2024-01-15", "laptop",  1200.0, "electronics"),
    (2, "2024-01-20", "phone",    800.0, "electronics"),
    (3, "2024-02-10", "t-shirt",   30.0, "clothing"),
    (4, "2024-02-15", "jeans",     50.0, "clothing"),
    (5, "2024-03-10", "tablet",   600.0, "electronics"),
    (6, "2024-03-20", "jacket",   120.0, "clothing"),
]

schema = StructType([
    StructField("id",        IntegerType(), True),
    StructField("sale_date", StringType(),  True),
    StructField("product",   StringType(),  True),
    StructField("amount",    DoubleType(),  True),
    StructField("category",  StringType(),  True),
])

df = spark.createDataFrame(data, schema)

# 2. Трансформации
df_transformed = (
    df
    .withColumn("amount",    F.col("amount").cast("double"))
    .withColumn("sale_date", F.to_date(F.col("sale_date")))
    .withColumn("year",      F.year(F.col("sale_date")).cast("string"))
    .withColumn("month",     F.lpad(F.month(F.col("sale_date")).cast("string"), 2, "0"))
)

df_transformed.show()

# 3. Агрегация
df_agg = (
    df_transformed
    .groupBy("category", "year", "month")
    .agg(
        F.sum("amount").alias("total_amount"),
        F.count("id").alias("total_sales"),
        F.avg("amount").alias("avg_amount")
    )
)

df_agg.show()

# 4. Сохраняем с партиционированием как таблицу Hive
writer = DBWriter(
    connection=hive,
    table="default.sales_aggregated",
    options=Hive.WriteOptions(
        mode="overwrite",
        partitionBy=["category", "year", "month"]
    )
)
writer.run(df_agg)
```

### Проверка
После можно проверить в Hive
```
beeline -u jdbc:hive2://jn:5433 -n scott tiger
```

```
SHOW TABLES;
SELECT * FROM sales_aggregated;
```

А так же прокинуть порты и посмотреть в UI
```
ssh -i .ssh/your_key -L 9870:192.168.10.31:9870 -L 8088:192.168.10.31:8088 -L 19888:192.168.10.31:19888 -L 10002:192.168.10.22:10002 ubuntu@178.236.25.105
```

Зайти в файловую систему и увидеть, что действительно создалась таблица с партиционированием

### Бонус

Так же можно посмотреть и на уже созданные таблицы до этого
Опять-таки нужно зайти в ipython
```
from onetl.connection import Hive
from onetl.db import DBWriter, DBReader
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.master("yarn")
    .appName("spark_check_yarn")
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
    .config("spark.hive.metastore.uris", "thrift://nn:9083")
    .enableHiveSupport()
    .getOrCreate()
)

hive = Hive(spark=spark, cluster="x")
hive.check()

reader = DBReader(connection=hive, table="default.sales_partitioned")
df = reader.run()

# Например, посчитать количество строк
df.count()

# Посмотреть схему
df.printSchema()

# Количество партиций
df.rdd.getNumPartitions()

# Вывести строки
df.show(3)
```
