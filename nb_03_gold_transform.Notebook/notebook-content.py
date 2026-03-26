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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

slv_customers = spark.table("slv_customers")
slv_orders = spark.table("slv_orders")
slv_order_items = spark.table("slv_order_items")
slv_payments = spark.table("slv_payments")
slv_products = spark.table("slv_products")
slv_reviews = spark.table("slv_reviews")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gld_dim_customer = (
    slv_customers
    .select(
        "customer_id",
        "customer_unique_id",
        "customer_city",
        "customer_state"
    )
    .dropDuplicates(["customer_id"])
    .withColumn("created_ts", F.current_timestamp())
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gld_dim_customer.write.format("delta").mode("overwrite").saveAsTable("gld_dim_customer")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.table("gld_dim_customer").show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gld_dim_product = (
    slv_products
    .select(
        "product_id",
        "product_category_name"
    )
    .dropDuplicates(["product_id"])
    .withColumn("created_ts", F.current_timestamp())
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gld_dim_product.write.format("delta").mode("overwrite").saveAsTable("gld_dim_product")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.table("gld_dim_product").show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dates = (
    slv_orders
    .select(F.to_date("order_purchase_timestamp").alias("order_date"))
    .filter(F.col("order_date").isNotNull())
    .dropDuplicates()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gld_dim_date = (
    dates
    .withColumn("year", F.year("order_date"))
    .withColumn("month", F.month("order_date"))
    .withColumn("day", F.dayofmonth("order_date"))
    .withColumn("day_of_week", F.dayofweek("order_date"))
    .withColumn("created_ts", F.current_timestamp())
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gld_dim_date.write.format("delta").mode("overwrite").saveAsTable("gld_dim_date")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.table("gld_dim_date").show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

payments_agg = (
    slv_payments
    .groupBy("order_id")
    .agg(
        F.sum("payment_value").alias("total_payment_value"),
        F.max("payment_type").alias("payment_type"),
        F.sum("payment_installments").alias("total_installments")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

payments_agg.show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gld_fact_sales = (
    slv_order_items.alias("oi")
    .join(slv_orders.alias("o"), F.col("oi.order_id") == F.col("o.order_id"), "inner")
    .join(payments_agg.alias("p"), F.col("oi.order_id") == F.col("p.order_id"), "left")
    .select(
        F.col("oi.order_id").alias("order_id"),
        F.col("oi.order_item_id").alias("order_item_id"),
        F.col("o.customer_id").alias("customer_id"),
        F.col("oi.product_id").alias("product_id"),
        F.to_date(F.col("o.order_purchase_timestamp")).alias("order_date"),
        F.col("o.order_status").alias("order_status"),
        F.col("oi.price").alias("unit_price"),
        F.lit(1).alias("quantity"),
        F.col("oi.freight_value").alias("freight_value"),
        F.col("p.total_payment_value").alias("payment_value"),
        F.col("p.payment_type").alias("payment_type"),
        F.col("p.total_installments").alias("payment_installments")
    )
    .withColumn("sales_amount", F.col("unit_price") * F.col("quantity"))
    .withColumn("created_ts", F.current_timestamp())
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gld_fact_sales.show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gld_fact_sales.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gld_fact_sales.write.format("delta").mode("overwrite").saveAsTable("gld_fact_sales")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.table("gld_fact_sales").show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gld_fact_reviews = (
    slv_reviews.alias("r")
    .join(slv_orders.alias("o"), F.col("r.order_id") == F.col("o.order_id"), "left")
    .select(
        F.col("r.review_id"),
        F.col("r.order_id"),
        F.col("o.customer_id"),
        F.to_date(F.col("o.order_purchase_timestamp")).alias("order_date"),
        F.col("r.review_score"),
        F.col("r.review_comment_title"),
        F.col("r.review_comment_message"),
        F.col("r.review_creation_date"),
        F.col("r.review_answer_timestamp")
    )
    .withColumn("created_ts", F.current_timestamp())
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gld_fact_reviews.write.format("delta").mode("overwrite").saveAsTable("gld_fact_reviews")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.table("gld_dim_customer").count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.table("gld_dim_product").count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.table("gld_dim_date").count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.table("gld_fact_sales").count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.table("gld_fact_reviews").count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
