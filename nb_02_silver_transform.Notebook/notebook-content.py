# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "60d1e55b-ad26-40c0-b3d3-39c8508f5b41",
# META       "default_lakehouse_name": "e_commerce_data",
# META       "default_lakehouse_workspace_id": "9672d9c4-5d16-481f-bb62-d8e5f047ca49",
# META       "known_lakehouses": [
# META         {
# META           "id": "60d1e55b-ad26-40c0-b3d3-39c8508f5b41"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

brz_customers = spark.table("brz_customers")
brz_orders = spark.table("brz_orders")
brz_order_items = spark.table("brz_order_items")
brz_payments = spark.table("brz_payments")
brz_products = spark.table("brz_products")
brz_reviews = spark.table("brz_reviews")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

brz_orders.show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

customers = (
    brz_customers
    .select(
        "customer_id",
        "customer_unique_id",
        "customer_zip_code_prefix",
        "customer_city",
        "customer_state",
        "ingestion_ts"
    )
    .withColumn("customer_city", F.initcap(F.trim(F.col("customer_city"))))
    .withColumn("customer_state", F.upper(F.trim(F.col("customer_state"))))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

w = Window.partitionBy("customer_id").orderBy(F.col("ingestion_ts").desc())

slv_customers = (
    customers
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

slv_customers.write.format("delta").mode("overwrite").saveAsTable("slv_customers")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.table("slv_customers").show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

orders = (
    brz_orders
    .select(
        "order_id",
        "customer_id",
        "order_status",
        "order_purchase_timestamp",
        "ingestion_ts"
    )
    .withColumn("order_status", F.lower(F.trim(F.col("order_status"))))
    .withColumn("order_purchase_timestamp", F.to_timestamp("order_purchase_timestamp"))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

w = Window.partitionBy("order_id").orderBy(F.col("ingestion_ts").desc())

slv_orders = (
    orders
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

slv_orders.write.format("delta").mode("overwrite").saveAsTable("slv_orders")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.table("slv_orders").count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

order_items = (
    brz_order_items
    .select(
        "order_id",
        "order_item_id",
        "product_id",
        "seller_id",
        "shipping_limit_date",
        "price",
        "freight_value",
        "ingestion_ts"
    )
    # Fix data types
    .withColumn("order_item_id", F.col("order_item_id").cast("int"))
    .withColumn("shipping_limit_date", F.to_timestamp("shipping_limit_date"))
    .withColumn("price", F.col("price").cast("double"))
    .withColumn("freight_value", F.col("freight_value").cast("double"))

    # Remove bad data
    .filter(F.col("price").isNotNull())
    .filter(F.col("price") >= 0)
    .filter(F.col("freight_value") >= 0)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

order_items.show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

w = Window.partitionBy("order_id", "order_item_id") \
          .orderBy(F.col("ingestion_ts").desc())

slv_order_items = (
    order_items
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

slv_order_items = (
    slv_order_items
    .withColumn("silver_load_ts", F.current_timestamp())
    .withColumn("record_source", F.lit("kaggle_olist"))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

slv_order_items.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("slv_order_items")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.table("slv_order_items").show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.table("slv_order_items").count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window

payments = (
    brz_payments
    .select(
        "order_id",
        "payment_sequential",
        "payment_type",
        "payment_installments",
        "payment_value",
        "ingestion_ts"
    )
    .withColumn("payment_sequential", F.col("payment_sequential").cast("int"))
    .withColumn("payment_installments", F.col("payment_installments").cast("int"))
    .withColumn("payment_value", F.col("payment_value").cast("double"))
    .withColumn("payment_type", F.lower(F.trim(F.col("payment_type"))))
    .filter(F.col("payment_value").isNotNull())
    .filter(F.col("payment_value") >= 0)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

w = Window.partitionBy("order_id", "payment_sequential").orderBy(F.col("ingestion_ts").desc())

slv_payments = (
    payments
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

slv_payments = (
    slv_payments
    .withColumn("silver_load_ts", F.current_timestamp())
    .withColumn("record_source", F.lit("kaggle_olist"))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

slv_payments.write.format("delta").mode("overwrite").saveAsTable("slv_payments")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.table("slv_payments").show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.table("slv_payments").count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

products = (
    brz_products
    .select(
        "product_id",
        "product_category_name",
        "product_name_lenght",
        "product_description_lenght",
        "product_photos_qty",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm",
        "ingestion_ts"
    )
    .withColumn("product_category_name", F.lower(F.trim(F.col("product_category_name"))))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

w = Window.partitionBy("product_id").orderBy(F.col("ingestion_ts").desc())

slv_products = (
    products
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

slv_products = (
    slv_products
    .withColumn("silver_load_ts", F.current_timestamp())
    .withColumn("record_source", F.lit("kaggle_olist"))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

slv_products.write.format("delta").mode("overwrite").saveAsTable("slv_products")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.table("slv_products").show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.table("slv_products").count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window

reviews = (
    brz_reviews
    .select(
        "review_id",
        "order_id",
        "review_score",
        "review_comment_title",
        "review_comment_message",
        "review_creation_date",
        "review_answer_timestamp",
        "ingestion_ts"
    )
    .withColumn("review_score", F.col("review_score").cast("int"))
    .withColumn("review_creation_date", F.to_timestamp("review_creation_date"))
    .withColumn("review_answer_timestamp", F.to_timestamp("review_answer_timestamp"))
    .filter(F.col("review_score").between(1, 5))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

w = Window.partitionBy("review_id").orderBy(F.col("ingestion_ts").desc())

slv_reviews = (
    reviews
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

slv_reviews = (
    slv_reviews
    .withColumn("silver_load_ts", F.current_timestamp())
    .withColumn("record_source", F.lit("kaggle_olist"))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

slv_reviews.write.format("delta").mode("overwrite").saveAsTable("slv_reviews")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.table("slv_reviews").show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.table("slv_reviews").count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
