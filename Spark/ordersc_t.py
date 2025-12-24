from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# SparkSession creation
spark = SparkSession.builder.appName('Orders').getOrCreate()

#Reading the file
df = spark.read.csv(
    r"C:\Users\prath\Downloads\orders.csv",
    header=True,
    inferSchema=True
)

df.show()
df.printSchema()

#Data cleaning

#drop duplicates
df = df.dropna(subset=["order_id", "order_customer_id", "order_status", "sum"])

#standardize date
df = df.withColumn("order_date", to_date("order_date", "dd-MM-yyyy"))


#Fixing inconsistent text format
df = df.withColumn("order_status", upper(trim(col("order_status")))) 

#type conversion
df = df.withColumn("order_id", col("order_id").cast("int")) \
       .withColumn("order_customer_id", col("order_customer_id").cast("int")) \
       .withColumn("sum", col("sum").cast("double"))

#removing invalid records
df = df.filter(col("order_id") > 0) \
       .filter(col("order_customer_id") > 0) \
       .filter(col("sum") >= 0)

#date validation
max_allowed_date = current_date()
df = df.filter(col("order_date") <= max_allowed_date)

df.show()
df.printSchema()

#Rank customers based on total revenue


ranked_customers = (
    df.groupBy("order_customer_id")
      .agg(sum("sum").alias("total_revenue"))
      .withColumn("revenue_rank", dense_rank().over(Window.orderBy(desc("total_revenue"))))
)

ranked_customers.show()

#How do you pivot order status so each status becomes a column

pivot_df = (
    df.groupBy("order_customer_id")
      .pivot("order_status")
      .agg(sum("sum"))
      .na.fill(0)
)

pivot_df.show()

#monthly revenue trends


monthly_revenue = (
    df.withColumn("order_month", date_format("order_date", "yyyy-MM"))
      .groupBy("order_month")
      .agg(sum("sum").alias("monthly_revenue"))
      .orderBy("order_month")
)
monthly_revenue.show()

#identify customers who have churned (no orders in last X days)?


x=30

last_orders = (
    df.groupBy("order_customer_id")
      .agg(max("order_date").alias("last_order_date"))
) # groups customers and find the last order

latest_date = df.agg(max("order_date")).first()[0]

churned_customers = last_orders.withColumn(
    "days_since_last_order",
    datediff(lit(latest_date), col("last_order_date"))
).filter(col("days_since_last_order") > x)

churned_customers.show()

spark.stop()

