from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("EcommerceStreaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# =========================
# Define Schema
# =========================
schema = StructType([
    StructField("event_id", StringType()),
    StructField("order_id", IntegerType()),
    StructField("user_id", IntegerType()),
    StructField("product_category", StringType()),
    StructField("price", DoubleType()),
    StructField("quantity", IntegerType()),
    StructField("payment_method", StringType()),
    StructField("order_status", StringType()),
    StructField("event_timestamp", StringType())
])

# =========================
# Read from Kafka
# =========================
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "ecommerce.orders.events") \
    .option("startingOffsets", "latest") \
    .load()

# =========================
# Bronze Layer
# =========================
bronze_df = df.selectExpr("CAST(value AS STRING) as raw_json")

bronze_query = bronze_df.writeStream \
    .format("json") \
    .option("path", "/opt/app/data/bronze") \
    .option("checkpointLocation", "/opt/app/data/checkpoint/bronze") \
    .outputMode("append") \
    .start()

# =========================
# Silver Layer
# =========================
json_df = df.selectExpr("CAST(value AS STRING)")

parsed_df = json_df.select(
    from_json(col("value"), schema).alias("data")
).select("data.*")

clean_df = parsed_df \
    .withColumn("event_time", to_timestamp("event_timestamp")) \
    .withColumn("revenue", col("price") * col("quantity")) \
    .drop("event_timestamp") \
    .filter(col("event_time").isNotNull())

silver_query = clean_df.writeStream \
    .format("parquet") \
    .option("path", "/opt/app/data/silver") \
    .option("checkpointLocation", "/opt/app/data/checkpoint/silver") \
    .outputMode("append") \
    .trigger(processingTime="1 minute") \
    .start()

# =========================
# GOLD LAYER → POSTGRES
# =========================
def write_revenue_to_postgres(batch_df, batch_id):
   
    if batch_df.rdd.isEmpty():
        return

    batch_df \
        .selectExpr(
            "window.start as window_start",
            "window.end as window_end",
            "total_revenue"
        ) \
        .write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/realtime_db") \
        .option("dbtable", "revenue_per_minute") \
        .option("user", "admin") \
        .option("password", "admin") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()


gold_revenue = clean_df \
    .withWatermark("event_time", "1 minute") \
    .groupBy(window("event_time", "1 minute")) \
    .agg(sum("revenue").alias("total_revenue"))
    
    
#revenue per category

gold_revenue_query = gold_revenue.writeStream \
    .foreachBatch(write_revenue_to_postgres) \
    .outputMode("append") \
    .trigger(processingTime="1 minute") \
    .start()
def write_category_to_postgres(batch_df, batch_id):
    batch_df.selectExpr(
        "window.start as window_start",
        "window.end as window_end",
        "product_category",
        "category_revenue"
    ).write \
     .format("jdbc") \
     .option("url", "jdbc:postgresql://postgres:5432/realtime_db") \
     .option("dbtable", "revenue_per_category") \
     .option("user", "admin") \
     .option("password", "admin") \
     .option("driver", "org.postgresql.Driver") \
     .mode("append") \
     .save()

gold_category = clean_df \
    .withWatermark("event_time", "1 minute") \
    .groupBy(window("event_time", "1 minute"), col("product_category")) \
    .agg(sum("revenue").alias("category_revenue"))

gold_category_query = gold_category.writeStream \
    .foreachBatch(write_category_to_postgres) \
    .outputMode("append") \
    .trigger(processingTime="1 minute") \
    .start()
    
# orders per payment 

def write_payment_to_postgres(batch_df, batch_id):
    batch_df.selectExpr(
        "window.start as window_start",
        "window.end as window_end",
        "payment_method",
        "order_count"
    ).write \
     .format("jdbc") \
     .option("url", "jdbc:postgresql://postgres:5432/realtime_db") \
     .option("dbtable", "orders_per_payment") \
     .option("user", "admin") \
     .option("password", "admin") \
     .option("driver", "org.postgresql.Driver") \
     .mode("append") \
     .save()

gold_payment = clean_df \
    .withWatermark("event_time", "1 minute") \
    .groupBy(window("event_time", "1 minute"), col("payment_method")) \
    .agg(count("*").alias("order_count"))

gold_payment_query = gold_payment.writeStream \
    .foreachBatch(write_payment_to_postgres) \
    .outputMode("append") \
    .trigger(processingTime="1 minute") \
    .start()
    
    
#total orders per minute
def write_total_orders(batch_df, batch_id):
    batch_df.selectExpr(
        "window.start as window_start",
        "window.end as window_end",
        "total_orders"
    ).write \
     .format("jdbc") \
     .option("url", "jdbc:postgresql://postgres:5432/realtime_db") \
     .option("dbtable", "total_orders_per_minute") \
     .option("user", "admin") \
     .option("password", "admin") \
     .option("driver", "org.postgresql.Driver") \
     .mode("append") \
     .save()

gold_total_orders = clean_df \
    .withWatermark("event_time", "1 minute") \
    .groupBy(window("event_time", "1 minute")) \
    .agg(count("*").alias("total_orders"))

gold_total_orders_query = gold_total_orders.writeStream \
    .foreachBatch(write_total_orders) \
    .outputMode("append") \
    .trigger(processingTime="1 minute") \
    .start()
    
    
#average order value per minute
def write_avg_order(batch_df, batch_id):
    batch_df.selectExpr(
        "window.start as window_start",
        "window.end as window_end",
        "avg_order_value"
    ).write \
     .format("jdbc") \
     .option("url", "jdbc:postgresql://postgres:5432/realtime_db") \
     .option("dbtable", "avg_order_value") \
     .option("user", "admin") \
     .option("password", "admin") \
     .option("driver", "org.postgresql.Driver") \
     .mode("append") \
     .save()

gold_avg = clean_df \
    .withWatermark("event_time", "1 minute") \
    .groupBy(window("event_time", "1 minute")) \
    .agg(avg("revenue").alias("avg_order_value"))

gold_avg_query = gold_avg.writeStream \
    .foreachBatch(write_avg_order) \
    .outputMode("append") \
    .trigger(processingTime="1 minute") \
    .start()
    
# Wait for termination
spark.streams.awaitAnyTermination()