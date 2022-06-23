from pyspark.sql import SparkSession


if __name__=="__main__":
    spark = SparkSession.builder.appName("spark_streaming").config("spark.sql.shuffle.partitions",3)\
        .config("spark.streaming.stopGracefullyOnShutdown",True)\
        .config("spark.sql.streaming.schemaInference",True)\
        .getOrCreate()

    df = spark.readStream.format("json").option("path","InputFolder").option("maxFilesPerTrigger",1).load()


    df.createOrReplaceTempView("Orders")

    df_= spark.sql("select * from Orders where order_status = 'COMPLETE'")
    
    ##write to console
    result = df_.writeStream.format("json").outputMode("append")\
        .option("path","myoutputFolder")\
        .option("checkpointLocation","checkppoint-location5")\
        .trigger(processingTime='30 seconds')\
        .start()


    result.awaitTermination()


