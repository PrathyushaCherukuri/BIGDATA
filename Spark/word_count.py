from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder.appName("app1").getOrCreate()

    rdd1 = spark.sparkContext.textFile(r"C:\Users\prath\Desktop\TRAINING\sqoop.txt")

    rdd2 = rdd1.flatMap(lambda x: x.split(" "))
    rdd3 = rdd2.map(lambda x: (x, 1))
    rdd4 = rdd3.reduceByKey(lambda x, y: x + y)

    print(f"Hadoop version = {spark.sparkContext._jvm.org.apache.hadoop.util.VersionInfo.getVersion()}")

    for ele in rdd4.collect():
        print(ele)

    spark.stop()
