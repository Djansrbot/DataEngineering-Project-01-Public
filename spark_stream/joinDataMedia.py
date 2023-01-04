# Not working as intended

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def quiet_logs(sc):
  logger = sc._jvm.org.apache.log4j
  logger.LogManager.getLogger("org"). setLevel(logger.Level.ERROR)
  logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

spark = SparkSession \
    .builder \
    .appName("joinDataMedia") \
    .getOrCreate()

quiet_logs(spark)

# ----------------------------------------------------------------

#Data schema
dataSchema = StructType([
  StructField('public_metrics', StructType([
        StructField('retweet_count', IntegerType(), True),
        StructField('reply_count', IntegerType(), True),
        StructField('like_count', IntegerType(), True),
        StructField('quote_count', IntegerType(), True)
  ])),
  StructField('author_id', StringType(), True),
  StructField('attachments', StructType([
        StructField('media_keys', ArrayType(StringType())
  )])),
  StructField('id', StringType(), True),
  StructField('created_at', TimestampType(), True),
  StructField('text', StringType(), True)
])

# media schema
mediaSchema = StructType([
    StructField('media_key', StringType(), True),
    StructField('type', StringType(), True)
  ])

# ----------------------------------------------------------------

dataStream = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "kafka1:19092") \
  .option("subscribe", "dataStream01") \
  .option("startingOffsets", "earliest") \
  .load()

mediaStream = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "kafka1:19092") \
  .option("subscribe", "media_keyStream01") \
  .option("startingOffsets", "earliest") \
  .load()

df2 = dataStream.selectExpr("CAST(key AS STRING)","CAST(value AS STRING)","CAST(timestamp AS TIMESTAMP)")
df3 = mediaStream.selectExpr("CAST(key AS STRING)","CAST(value AS STRING)","CAST(timestamp AS TIMESTAMP)")

# --------------------------------------------------------------------------------------------

#data columns
dataDf = df2.select(from_json(col("value"), dataSchema).alias("data"), col("timestamp")) \
.select(
  col("data.id").alias("tweet_id"),
  col("data.created_at").cast(TimestampType()).alias("created_at"),
  col("data.text").alias("text"),
  col("data.public_metrics.retweet_count").cast(IntegerType()).alias("retweet_count"),
  col("data.public_metrics.reply_count").cast(IntegerType()).alias("reply_count"),
  col("data.public_metrics.like_count").cast(IntegerType()).alias("like_count"),
  col("data.public_metrics.quote_count").cast(IntegerType()).alias("quote_count"),
  col("data.author_id").alias("author_id"),
  col("data.attachments.media_keys").alias("data_media_keys"),
  col("timestamp").alias("dataTimestamp")
).withColumn("media_key", explode_outer("data_media_keys")).drop("data_media_keys")

#media columns
mediaDf = df3.select(from_json(col("value"), mediaSchema).alias("media"), col("timestamp")) \
.select( 
  col("media.media_key").alias("media_key"),
  col("media.type").alias("media_type"),
  col("timestamp").alias("mediaTimestamp")
)

# ------------------------------------------------------------------------------------

## Sink to HDFS
# query =  userDf \
#     .writeStream \
#     .outputMode("append") \
#     .format("json") \
#     .option("path","hdfs://namenode:9000/srdjan/joinDataMedia") \
#     .option("checkpointLocation", "file:/spark-warehouse/tmp/joinDataMedia") \
#     .option("failOnDataLoss", "false") \
#     .start() \
#     .awaitTermination()


## To console
# query = userDf \
#     .writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .start() \
#     .awaitTermination()
##.option("truncate", "false") \
##.trigger(processingTime='2 seconds') \


## Sink to postgresql
# def foreach_batch_function(df, epoch_id):
#     df.write \
#     .format("jdbc") \
#     .option("driver", "org.postgresql.Driver") \
#     .option("url", "jdbc:postgresql://postgres:5432/postgres") \
# 	  .option("dbtable", "joinDataMedia") \
# 	  .option("user", "postgres") \
#     .option("password", "postgres") \
#     .mode("append")\
#     .save()
# userDf.writeStream.foreachBatch(foreach_batch_function).start() \
# .awaitTermination()
##.outputMode("append")


# Sink on kafka topic
# query = userDf \
#     .selectExpr("CAST(users_id AS STRING) AS key", "to_json(struct(*)) AS value") \
#     .writeStream \
#     .format("kafka") \
#     .outputMode("append") \
#     .option("failOnDataLoss", "false") \
#     .option("kafka.bootstrap.servers","kafka1:19092") \
#     .option("topic","joinDataMedia") \
#     .option("checkpointLocation", "file:/spark-warehouse/joinDataMedia") \
#     .start() \
#     .awaitTermination()


## run app:
# ./spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 --jars /home/driver/postgresql-42.3.5.jar /home/stream_zadaci/joinDataMedia.py

## run kafka consumer
# usr/bin/kafka-console-consumer --bootstrap-server localhost:9092 --topic joinDataMedia --from-beginning