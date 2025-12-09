from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, sum as spark_sum, countDistinct, when

spark = SparkSession.builder.appName("Orders").getOrCreate()

# Read CSV
df = spark.read.option("header", "true").option("inferSchema", "true") \
    .csv(r"C:\Users\prath\Desktop\TRAINING\Spark\orders.csv")  # raw string for Windows path

# Convert order_date to date type
df1 = df.withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd"))

# Rename column 'sum' to 'order_amount' if it exists
df2 = df1.withColumnRenamed("sum", "order_amount")

# Filter only completed orders
df3 = df2.filter(col("order_status") == "COMPLETE")

# Flag high value orders
df4 = df3.withColumn("high_value_flag", when(col("order_amount") > 2500, "YES").otherwise("NO"))

# Group by customer and aggregate
df5 = df4.groupBy("order_customer_id").agg(
    spark_sum("order_amount").alias("total_spent"),
    countDistinct("order_id").alias("total_orders")
)

# Show result
df5.show()
