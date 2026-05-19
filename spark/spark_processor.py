from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, sum as _sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

# Configurações
KAFKA_BOOTSTRAP_SERVERS = 'kafka:29092'
KAFKA_TOPIC = 'ecommerce_sales'
CHECKPOINT_LOCATION = '/tmp/spark-checkpoints'
POSTGRES_URL = "jdbc:postgresql://postgres:5432/ecommerce_db"
POSTGRES_PROPERTIES = {
    "user": "user",
    "password": "password",
    "driver": "org.postgresql.Driver"
}

# Schema dos dados de entrada (Camada Bronze)
schema = StructType([
    StructField("order_id", IntegerType()),
    StructField("customer_id", IntegerType()),
    StructField("product_name", StringType()),
    StructField("category", StringType()),
    StructField("price", DoubleType()),
    StructField("quantity", IntegerType()),
    StructField("timestamp", StringType())
])

def save_to_postgres(batch_df, batch_id):
    """
    Função auxiliar para salvar cada micro-batch no PostgreSQL.
    """
    # Flatten do window para salvar colunas separadas
    final_df = batch_df.select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("category"),
        col("total_sales")
    )
    
    final_df.write \
        .jdbc(url=POSTGRES_URL, table="sales_summary", mode="append", properties=POSTGRES_PROPERTIES)

def main():
    # Adicionando o JAR do driver do Postgres nas dependências
    spark = SparkSession.builder \
        .appName("EcommerceSalesStreaming") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.1") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # 1. Leitura do Kafka (Camada Bronze)
    raw_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    # 2. Parsing e Casting (Camada Prata)
    silver_df = raw_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*") \
        .withColumn("timestamp", col("timestamp").cast(TimestampType())) \
        .withColumn("total_value", col("price") * col("quantity")) \
        .dropDuplicates(["order_id"])

    # 3. Agregação de métricas (Camada Ouro)
    gold_df = silver_df \
        .withWatermark("timestamp", "1 minute") \
        .groupBy(
            window(col("timestamp"), "1 minute"),
            col("category")
        ) \
        .agg(_sum("total_value").alias("total_sales"))

    # Output 1: Console (para debug)
    query_console = gold_df.writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", "false") \
        .start()

    # Output 2: PostgreSQL (Persistência da camada de ouro)
    query_postgres = gold_df.writeStream \
        .foreachBatch(save_to_postgres) \
        .outputMode("update") \
        .option("checkpointLocation", CHECKPOINT_LOCATION) \
        .start()

    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
