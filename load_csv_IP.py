# sudo airmon-ng start wlp0s20f3
# sudo airmon-ng stop wlp0s20f3mon

from kafka import KafkaProducer
import pandas as pd
import numpy as np
# import dask.dataframe as dd
import time

df = pd.read_csv('/home/jaswanth/Documents/BDA/Sem-5/Project/Data/Dataset_IP.csv')

producer = KafkaProducer(bootstrap_servers='127.0.0.1:9092',key_serializer=str.encode,value_serializer=str.encode)

for row in df.values:
    temp = np.ndarray.tolist(row)
    temp_ = str(temp).replace('[','').replace(']','')
    #print(type(temp_))
    producer.send("IP",key='dummy1',value=temp_)
    time.sleep(0.07)
    
    
# dff = spark.sql("SELECT * FROM gowiththeflow")

# import pandas as pd
# from pyspark.sql.types import IntegerType
# df1 = pd.DataFrame(df, index = ["timestamp","source_ip","dest_ip","port","bytes"])

# df = spark.read.csv("/home/jaswanth/Documents/BDA/Sem-5/Project/Data/test1.csv")
# df = df.withColumnRenamed("_c0","timestamp")
# df = df.withColumnRenamed("_c1","source_ip")
# df = df.withColumnRenamed("_c2","dest_ip")
# df = df.withColumnRenamed("_c3","port")
# df = df.withColumnRenamed("_c4","bytes")
# df = df.drop("port")
# df = df.withColumn("bytes",df["bytes"].cast(IntegerType()))
# df.write.format("mongo").mode("append").option("database","project").option("collection","network").option("uri","mongodb://127.0.0.1:27017/").save()
# df = spark.read.format("mongo").option("database","project").option("collection","network").option("uri","mongodb://127.0.0.1:27017/").load()


# ./pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1,org.mongodb.spark:mongo-spark-connector_2.12:3.0.1