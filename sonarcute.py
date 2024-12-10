# Databricks notebook source
from pyspark.sql.functions import col, when, lit, sum as spark_sum

# Sample DataFrame
data = [
    (1, "A", 100),
    (2, "B", 200),
    (3, "A", 300),
    (4, "B", 400),
    (5, "A", 500)
]

df = spark.createDataFrame(data, ["id", "category", "value"])
df.createOrReplaceTempView("temp_table")

# Complex transformation
df = df.withColumn("value_category", when(col("category") == "A", col("value") * 2).otherwise(col("value") / 2))
df.createOrReplaceTempView("temp_table_with_value_category")

df = spark.sql("""
    SELECT category, 
           SUM(value) AS total_value, 
           SUM(value_category) AS total_value_category 
    FROM temp_table_with_value_category 
    GROUP BY category
""")
df.createOrReplaceTempView("temp_table_grouped")

df = spark.sql("""
    SELECT *, 
           (total_value_category - total_value) AS value_difference 
    FROM temp_table_grouped
""")

diisplay(df)