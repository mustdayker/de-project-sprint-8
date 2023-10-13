# ___________________ БЛОК ИМПОРТОВ ___________________

import os

from datetime import datetime
from time import sleep
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.functions import from_json, to_json, col, lit, struct
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType, LongType
import psycopg2


# ___________________ БЛОК ПЕРЕМЕННЫХ ___________________

# необходимые библиотеки для интеграции Spark с Kafka и PostgreSQL
spark_jars_packages = ",".join(
        [
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
            "org.postgresql:postgresql:42.4.0",
        ]
    )

topic_in = 'mustdayker_in' # входной топик
topic_out = 'mustdayker_out' # выходной топик

kafka_security_options = {
    'kafka.bootstrap.servers': 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091',
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'SCRAM-SHA-512',
    'kafka.sasl.jaas.config': 'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"de-student\" password=\"ltcneltyn\";'
}


# ___________________ СТАРТУЕМ СПАРК СЕССИЮ ___________________

# создаём spark сессию с необходимыми библиотеками в spark_jars_packages для интеграции с Kafka и PostgreSQL
spark = SparkSession.builder \
    .appName("RestaurantSubscribeStreamingService") \
    .config("spark.sql.session.timeZone", "UTC") \
    .config("spark.jars.packages", spark_jars_packages) \
    .getOrCreate()


# ___________________ СОЗДАЕМ ТАБЛИЦУ POSTGRES ___________________

URL = """ 
    host=localhost
    port=5432
    dbname=de
    user=jovyan
    password=jovyan
    target_session_attrs=read-write
"""
DDL = """ 
CREATE TABLE IF NOT EXISTS public.subscribers_feedback (
  id serial4 NOT NULL,
    restaurant_id text NOT NULL,
    adv_campaign_id text NOT NULL,
    adv_campaign_content text NOT NULL,
    adv_campaign_owner text NOT NULL,
    adv_campaign_owner_contact text NOT NULL,
    adv_campaign_datetime_start int8 NOT NULL,
    adv_campaign_datetime_end int8 NOT NULL,
    datetime_created int8 NOT NULL,
    client_id text NOT NULL,
    trigger_datetime_created int4 NOT NULL,
    feedback varchar NULL,
    CONSTRAINT id_pk PRIMARY KEY (id)
);
"""
CHECK_CREATION = """
SELECT EXISTS (
    SELECT 1
    FROM   information_schema.tables
    WHERE  table_schema = 'public'
    AND    table_name = 'subscribers_feedback'
);
"""

# создаем таблицу в postgres внутри docker контейнера
conn = psycopg2.connect(URL)
try:
    with conn:
        with conn.cursor() as cur:
            cur.execute(DDL)
finally:
    conn.close()

# проверяем создание таблицы
conn = psycopg2.connect(URL)
try:
    with conn:
        with conn.cursor() as cur:
            cur.execute( CHECK_CREATION )
            result = cur.fetchone()
finally:
    conn.close()
print(result)



# ___________________ ЧИТАЕМ ПОТОК ___________________

# читаем из топика Kafka сообщения с акциями от ресторанов 
restaurant_read_stream_df = spark.readStream \
    .format('kafka') \
    .options(**kafka_security_options) \
    .option("subscribe", topic_in)\
    .load()


# ___________________ ПРЕОБРАЗУЕМ ПОТОК ___________________


# определяем схему входного сообщения для json
incomming_message_schema = StructType([
    StructField("restaurant_id", StringType(), True),
    StructField("adv_campaign_id", StringType(), True),
    StructField("adv_campaign_content", StringType(), True),
    StructField("adv_campaign_owner", StringType(), True),
    StructField("adv_campaign_owner_contact", StringType(), True),
    StructField("adv_campaign_datetime_start", LongType(), True),
    StructField("adv_campaign_datetime_end", LongType(), True),
    StructField("datetime_created", LongType(), True)
])

# десериализуем из value сообщения json и фильтруем по времени старта и окончания акции
filtered_read_stream_df = (restaurant_read_stream_df
      .withColumn('value', f.col('value').cast(StringType()))
      .withColumn('key', f.col('key').cast(StringType()))
      .withColumn('event', f.from_json(f.col('value'), incomming_message_schema))
      .selectExpr('event.*', '*').drop('event')
      .withColumn('timestamp', f.unix_timestamp('timestamp'))
      .filter(f.col("timestamp").between(
            f.col("adv_campaign_datetime_start"),
            f.col("adv_campaign_datetime_end")))
        .select(
            'restaurant_id', 
            'adv_campaign_id', 
            'adv_campaign_content', 
            'adv_campaign_owner', 
            'adv_campaign_owner_contact', 
            'adv_campaign_datetime_start', 
            'adv_campaign_datetime_end', 
            'datetime_created'
            )
      )

# ___________________ ЧИТАЕМ СТАТИЧНЫЕ ДАННЫЕ ИЗ БД ___________________

# вычитываем всех пользователей с подпиской на рестораны
subscribers_restaurant_df = spark.read \
                    .format('jdbc') \
                    .option('url', 'jdbc:postgresql://rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de') \
                    .option('driver', 'org.postgresql.Driver') \
                    .option('dbtable', 'subscribers_restaurants') \
                    .option('user', 'student') \
                    .option('password', 'de-student') \
                    .load()\
                    .dropDuplicates(['restaurant_id', 'client_id'])


# ___________________ ДЖОЙНИМ ПОТОК С БД ___________________

# джойним данные из сообщения Kafka с пользователями подписки по restaurant_id (uuid). Добавляем время создания события.
result_df = filtered_read_stream_df\
    .join(
    subscribers_restaurant_df.select('restaurant_id', 'client_id'), 
    'restaurant_id', 
    'inner'
    )\
    .withColumn("trigger_datetime_created", lit(int(round(datetime.utcnow().timestamp()))))\
    .select(
        'restaurant_id', 
        'adv_campaign_id', 
        'adv_campaign_content', 
        'adv_campaign_owner', 
        'adv_campaign_owner_contact', 
        'adv_campaign_datetime_start', 
        'adv_campaign_datetime_end', 
        'client_id',
        'datetime_created',
        'trigger_datetime_created'
        )\
        .dropDuplicates(['restaurant_id', 'client_id', 'adv_campaign_id'])


# ___________________ ФУНКЦИЯ FOREBATCH ___________________


# метод для записи данных в 2 target: в PostgreSQL для фидбэков и в Kafka для триггеров

def foreach_batch_function(df, epoch_id):
    # сохраняем df в памяти, чтобы не создавать df заново перед отправкой в Kafka
    df.persist()

    # записываем df в PostgreSQL с полем feedback
    df.write \
        .mode("append") \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/de") \
        .option('driver', 'org.postgresql.Driver') \
        .option("dbtable", "subscribers_feedback") \
        .option("user", "jovyan") \
        .option("password", "jovyan") \
        .save()

    # создаём df для отправки в Kafka. Сериализация в json.
    kafka_df = result_df.select(f.to_json(f.struct(
        'restaurant_id', 
        'adv_campaign_id', 
        'adv_campaign_content', 
        'adv_campaign_owner', 
        'adv_campaign_owner_contact', 
        'adv_campaign_datetime_start', 
        'adv_campaign_datetime_end', 
        'client_id',
        'datetime_created',
        'trigger_datetime_created'
        )).alias('value'))

    # отправляем сообщения в результирующий топик Kafka без поля feedback
    kafka_df\
            .writeStream\
            .outputMode("append")\
            .format("kafka")\
            .options(**kafka_security_options)\
            .option("topic", topic_out)\
            .trigger(processingTime="15 seconds")\
            .option("checkpointLocation", "/root/spark_checkpoint")\
            .option("truncate", False)\
            .start()
    

    # очищаем память от df
    df.unpersist()


# ___________________ ОТПРАВКА ДАННЫХ В СТОК ___________________

result_df.writeStream \
    .foreachBatch(foreach_batch_function) \
    .start() \
    .awaitTermination()
