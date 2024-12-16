# !pip install pyspark

# import numpy as np
# import pandas as pd
import time
from pyspark.sql import SparkSession

"""
# .master("spark://master_ip:master_port") \
# Spark session oluşturma
spark = SparkSession.builder \
    .appName("PySpark-RDD List Processing") \
    .getOrCreate()

sc = spark.sparkContext  # SparkContext'e erişim
"""

start_time = time.time()
# Create a list
my_list = list(range(10000001))

# Convert list to RDD
rdd = sc.parallelize(my_list)
# do some calculation
processed_rdd = rdd.map(lambda x: x * 2)

result = processed_rdd.collect()

end_time = time.time()
elapsed_time = end_time - start_time

print(f"Çalışma süresi: {elapsed_time} saniye")
print(f"Cluster: {result}")

# stop sprak session when your code is finished.
# spark.stop()
