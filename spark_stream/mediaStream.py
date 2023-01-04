from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def quiet_logs(sc):
  logger = sc._jvm.org.apache.log4j
  logger.LogManager.getLogger("org"). setLevel(logger.Level.ERROR)
  logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

spark = SparkSession \
    .builder \
    .appName("mediaStream") \
    .getOrCreate()

quiet_logs(spark)

# ----------------------------------------------------------------

# Media Stream
mediaStream = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "kafka1:19092") \
  .option("subscribe", "mediaKey01") \
  .load()
## .option("startingOffsets", "earliest") \

df = mediaStream.selectExpr("CAST(key AS STRING)","CAST(value AS STRING)")

# ----------------------------------------------------------------

# Media schema
mediaSchema = StructType([
    StructField('media_key', StringType(), True),
    StructField('type', StringType(), True)
  ])

# ----------------------------------------------------------------

mediaDf = df.select(from_json(col("value"), mediaSchema).alias("media")) \
.select( 
  col("media.media_key").alias("media_key"),
  col("media.type").alias("media_type")
)

# ----------------------------------------------------------------

## Sink to HDFS
# query =  mediaDf \
#     .writeStream \
#     .outputMode("append") \
#     .format("json") \
#     .option("path","hdfs://namenode:9000/srdjan/mediaStream") \
#     .option("checkpointLocation", "file:/spark-warehouse/tmp/mediaStream") \
#     .start() \
#     .awaitTermination()


## To console
# query = mediaDf \
#     .writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .start() \
#     .awaitTermination()
##.option("truncate", "false") \
##.trigger(processingTime='2 seconds') \


## Sink to postgresql
def foreach_batch_function(df, epoch_id):
    df.write \
    .format("jdbc") \
    .option("driver", "org.postgresql.Driver") \
    .option("url", "jdbc:postgresql://postgres:5432/postgres") \
	  .option("dbtable", "mediaStream") \
	  .option("user", "postgres") \
    .option("password", "postgres") \
    .mode("append")\
    .save()
mediaDf.writeStream.foreachBatch(foreach_batch_function).start() \
.awaitTermination()
##.outputMode("append")


# Sink on kafka topic
# query = mediaDf \
#     .selectExpr("to_json(struct(*)) AS value") \
#     .writeStream \
#     .format("kafka") \
#     .outputMode("append") \
#     .option("kafka.bootstrap.servers","kafka1:19092") \
#     .option("topic","mediaStream") \
#     .option("checkpointLocation", "file:/spark-warehouse/mediaStream") \
#     .start() \
#     .awaitTermination()
## .option("failOnDataLoss", "false") \



## run app:
# ./spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 --jars /home/driver/postgresql-42.3.5.jar /home/stream_zadaci/mediaStream.py

## run kafka consumer
# usr/bin/kafka-console-consumer --bootstrap-server localhost:9092 --topic mediaStream --from-beginning