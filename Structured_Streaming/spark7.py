from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import functions as F

if __name__=="__main__":
    spark = SparkSession.builder.appName("spark_streaming").config("spark.sql.shuffle.partitions",3)\
        .config("spark.streaming.stopGracefullyOnShutdown",True)\
        .getOrCreate()

    df = spark.readStream.format("socket").option("host","localhost").option("port","9092").load()
    #df_.printSchema()

    schema = StructType([
    StructField("order_id",IntegerType()),
    StructField("order_date",TimestampType()),
    StructField("order_customer_id",IntegerType()),
    StructField("order_status",StringType()),
    StructField("amount",IntegerType())
   ])

    valueDf = df.select(from_json(F.col("value"),schema).alias("Value"))

    redefinedOrdersDf = valueDf.select("Value.*")

    #redefinedOrdersDf.printSchema()

    windowAggDF = redefinedOrdersDf\
        .withWatermark("order_date","30 minute")\
        .groupBy(window(F.col("order_date"),"15 minute")).agg(F.sum("amount").alias("totalInvoice"))

    resultDF  = windowAggDF.select("window.start","window.end","totalInvoice")

    orderQuery = resultDF.writeStream.format("console").outputMode("update").option("chckpointLocation","chk-location2")\
        .trigger(processingTime="15 second").start()

    orderQuery.awaitTermination() 



    
    




