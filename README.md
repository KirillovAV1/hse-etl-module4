# Практическая работа. Модуль 4

## Задание 1. Работа с Yandex DataTransfer

### 1. Создать БД Yandex DataBase.

Создаем БД в Managed Service for YDB. 
* Выбрал тип **Severless**, т.к. у него тарификация исходит из количества/сложности запросов пользователя.
* Тип нагрузки **OLTP**, потому что дашборды в datalens не будут напрямую брать данные из БД (соотвественно OLAP не нужен).

<img width="426" height="556" alt="image" src="https://github.com/user-attachments/assets/3a7354cd-8231-4219-bf40-e2b863707204" />
 
В [инструкции](https://yandex.cloud/ru/docs/data-transfer/tutorials/ydb-to-object-storage) указано, что предварительно нужно создать бакет в s3-хранилище и сервисный аккаунт. У меня они уже были, поэтому я только накинул дополнительные роли, которых не хватало сервисному аккаунту:

 <img width="512" height="441" alt="image" src="https://github.com/user-attachments/assets/96efc004-f510-4f60-80b1-9e9fdb5cda99" />

---

### 2. Подготовить данные.

В качестве датасета взял [transactions_v2](https://www.kaggle.com/datasets/bmurphmedia/transactions-v2) 

Создаем таблицу (01_createDB.sql):

<img width="715" height="402" alt="image" src="https://github.com/user-attachments/assets/185d9b3f-efe0-4363-a99e-f4d5eff556a3" />

Через YDB CLI загрузил данные в БД:

```bash
ydb --profile alkir  import file csv \
  --path transactions \
  --header \
  /Users/aleksandr/Downloads/transactions_v2.csv
```

<img width="1492" height="881" alt="image" src="https://github.com/user-attachments/assets/3b5da5d8-1169-43cc-863c-4ca943386108" />

---

### 3. Создать трансфер в Object Storage.

<img width="845" height="959" alt="image" src="https://github.com/user-attachments/assets/44548231-0179-445a-a491-5f1f57062c20" />

---

### 4. Проверить работоспособность трансфера

Трансфер корректно отработал:

<img width="1473" height="881" alt="image" src="https://github.com/user-attachments/assets/db165e78-a1d8-4270-9c54-3f9fd3c349ca" />

Результаты в S3:

<img width="868" height="315" alt="image" src="https://github.com/user-attachments/assets/0d368321-fec4-4359-ae2f-5ee5050da9d7" />

---

## Задание 2.Автоматизация работы с Yandex Data Processing при помощи Apache AirFlow.

В [документации](https://yandex.cloud/ru/docs/managed-airflow/tutorials/data-processing-automation) Предусмотрены два сценария:

* Упрощенная настройка
* Высокий уровень безопасности

Я выбрал **упрощенную настройку**

### 1. Подготовить инфраструктуру.

Часть инфраструктуры в плане настройки сетей, сервисного аккаунта были выполнены в предыдущих работах по предмету **Семинар наставника**, поэтому сюда прикреплю только новые сервисы:

Создание кластера Hive Metastore:

<img width="766" height="849" alt="image" src="https://github.com/user-attachments/assets/19c896ed-5291-440b-b64e-dd055f201281" />

Создание кластера Apache Airflow:

<img width="611" height="949" alt="image" src="https://github.com/user-attachments/assets/08c0e1fb-3278-4fb8-a474-45427d023c2d" />

---

### 2. Подготовить PySpark-задание.

В качестве датасета использовал [Online Retail II UCI](https://www.kaggle.com/datasets/mashlyn/online-retail-ii-uci)

<img width="906" height="234" alt="image" src="https://github.com/user-attachments/assets/b99faa8c-9d79-4649-9912-1f374b1562f1" />

Загрузил измененный из инструкции скрипт `create-table.py`, который считывает csv файл из s3, а затем cоздает Spark таблицу:

<img width="899" height="257" alt="image" src="https://github.com/user-attachments/assets/af3367ab-da09-4556-aace-14aa15e2a60d" />

```python
from pyspark.sql.types import *
from pyspark.sql import SparkSession


BUCKET = "s3-etl"

INPUT_PATH = f"s3a://{BUCKET}/task2/input/online_retail_II.csv"
OUTPUT_PATH = f"s3a://{BUCKET}/task2/output"

spark = SparkSession.builder \
    .appName("create-table") \
    .enableHiveSupport() \
    .getOrCreate()

schema = StructType([
    StructField("Invoice", StringType(), True),
    StructField("StockCode", StringType(), True),
    StructField("Description", StringType(), True),
    StructField("Quantity", IntegerType(), True),
    StructField("InvoiceDate", StringType(), True),
    StructField("Price", DoubleType(), True),
    StructField("Customer ID", LongType(), True),
    StructField("Country", StringType(), True)
])

df = spark.read.option("header", "true").schema(schema).csv(INPUT_PATH)

spark.sql("DROP TABLE IF EXISTS result")

df.write.mode("overwrite").option("path", OUTPUT_PATH).saveAsTable("result")
```

---

### 3. Подготовить DAG-файл, запустить и проверить результат

Загрузил заполненный DAG-файл из инструкции `Data-Processing-DAG.py`, но понизил количество ресурсов (иначе DAG падает с ошибкой, т.к. запрашиваемое число SSD памяти больше лимита):

<img width="949" height="242" alt="image" src="https://github.com/user-attachments/assets/825baa4b-bb37-455f-8e54-edfe85a6f1a2" />

Проверка результатов в UI Airflow:

<img width="1782" height="840" alt="image" src="https://github.com/user-attachments/assets/d690a5fe-18eb-4c2a-86f2-1bd926ffd8cf" />

Результаты в S3:

<img width="1220" height="372" alt="image" src="https://github.com/user-attachments/assets/cb48c779-7319-441d-beba-264a68077968" />

---

## Задание 3. Работа с топиками Apache Kafka® с помощью PySpark заданий в Yandex Data Processing.

