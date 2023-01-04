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
    .appName("joinDataUserMedia") \
    .getOrCreate()

quiet_logs(spark)

# ----------------------------------------------------------------

#kafka schema
# kafkaSchema = StructType([
#     StructField("key", BinaryType(), True),
#     StructField("value", BinaryType(), True),
#     StructField("topic", StringType(), True),
#     StructField("partition", IntegerType(), True),
#     StructField("offset", LongType(), True),
#     StructField("timestamp", TimestampType(), True),
#     StructField("timestampType", IntegerType(), True)
#   ])

# users schema
usersSchema = StructType([
    StructField('description', StringType(), True),
    StructField('username', StringType(), True),
    StructField('name', StringType(), True),
    StructField('id', StringType(), True)
])

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

usersStream = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "kafka1:19092") \
  .option("subscribe", "usersStream1") \
  .load()

dataStream = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "kafka1:19092") \
  .option("subscribe", "dataStream01") \
  .load()

mediaStream = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "kafka1:19092") \
  .option("subscribe", "mediaKey01") \
  .load()

## .option("startingOffsets", "earliest") \

df1 = usersStream.selectExpr("CAST(key AS STRING)","CAST(value AS STRING)","CAST(timestamp AS TIMESTAMP)")
df2 = dataStream.selectExpr("CAST(key AS STRING)","CAST(value AS STRING)","CAST(timestamp AS TIMESTAMP)")
df3 = mediaStream.selectExpr("CAST(key AS STRING)","CAST(value AS STRING)","CAST(timestamp AS TIMESTAMP)")

# --------------------------------------------------------------------------------------------

#user columns
userDf= df1.select(from_json(col("value"), usersSchema).alias("users"), col("timestamp")) \
.select( 
  col("users.username").alias("username"),
  col("users.description").alias("description"),
  col("users.id").alias("users_id"),
  col("users.name").alias("name"),
  col("timestamp").alias("userTimestamp")
).distinct()

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

# dt = df2.select(from_json(col("value"), dataSchema).alias("data"), col("timestamp"))
# test = dt.select(explode_outer("data.attachments.media_keys").alias("media_key"))
# test2 = mediaDf.select("*", when(col("media_key").isNull(), None).when(col("media_key").isNotNull(), col("media_key")))

user_wm = userDf.select("*").withWatermark("userTimestamp", "30 seconds").alias("A")
data_wm = dataDf.select("*").withWatermark("dataTimestamp", "30 seconds").drop("data_media_keys").alias("B")
media_wm = mediaDf.select("*").withWatermark("mediaTimestamp", "30 seconds").alias("C")

# joinDf2 = data_wm \
# .join(media_wm, expr("""
#     B.media_key <=> C.media_key  AND
#     B.dataTimestamp >= C.mediaTimestamp AND
#     B.dataTimestamp <= C.mediaTimestamp + interval 1 minutes
#   """), "left_outer") \
#   .select("author_id","text","B.media_key", "B.dataTimestamp")
  #.groupBy(window("B.dataTimestamp","20 seconds", "5 seconds"),"text").count()


joinDf3 = data_wm \
.join(user_wm, col("author_id") == col("users_id") , "inner") \
.join(media_wm, expr("""
    B.media_key <=> C.media_key  AND
    B.dataTimestamp >= C.mediaTimestamp AND
    B.dataTimestamp <= C.mediaTimestamp + interval 1 minutes
  """), "left_outer").select("B.text", "A.username", "C.media_key").distinct()

# user_wm = userDf.select("*").withWatermark("userTimestamp", "1 minutes").alias("A")
# data_wm = dataDf.select("*", explode_outer("data_media_keys").alias("media_key")).withWatermark("dataTimestamp", "1 minutes").drop("data_media_keys").alias("B")
# #\.withColumn("media_key2",when(col("media_key").isNull(), None).when(col("media_key").isNotNull(), col("media_key")))
# media_wm = mediaDf.select("*").withWatermark("mediaTimestamp", "1 minutes") \
# .withColumn("media_key2",when(col("media_key").isNull(), None).when(col("media_key").isNotNull(), col("media_key"))).alias("C")

# joinDf2 = data_wm \
# .join(media_wm, expr("""
#     B.media_key = C.media_key2  AND
#     B.dataTimestamp >= C.mediaTimestamp AND
#     B.dataTimestamp <= C.mediaTimestamp + interval 1 minutes
#   """), "left_outer") \
#   .select("*")



# joinDf2 = dataDf.withWatermark("dataTimestamp", "10 minutes") \
# .join(mediaDf.withWatermark("mediaTimestamp", "10 minutes"), "media_key", "leftOuter") \
# .select("media_key", "media_type", "created_at", "text")

# joinDf3 = data_wm \
# .join(user_wm, col("author_id") == col("users_id"), "inner") \
# .join(media_wm, expr("""
#     B.media_key = C.media_key AND
#     B.dataTimestamp >= C.mediaTimestamp AND
#     B.dataTimestamp <= C.mediaTimestamp + interval 1 minutes
#   """), "inner") \
# .select("author_id", "username", "B.media_key", "text", "username")


#photo,video,gif count group by window, name
# df3 = dataDf.withWatermark("dataTimestamp", "10 minutes") \
# .join(mediaDf.withWatermark("mediaTimestamp", "10 minutes"), "media_key", "full_outer") \
# .join(userDf.withWatermark("userTimestamp", "10 minutes"), col("author_id") == col("users_id"), "full_outer") \
# .groupBy(window("dataTimestamp","5 minutes", "2 minutes"), "username") \
# .agg(
#   count(when(col("media_type") == 'photo', '1')).alias("photo_count"),
#   count(when(col("media_type") == 'video', '1')).alias("video_count"),
#   count(when(col("media_type") == 'animated_gif', '1')).alias("gif_count")
# )
#test = dataStream.printSchema()

# media_wm = mediaDf.withWatermark("mediaTimestamp", "7 minutes")
# data_wm = dataDf.withWatermark("dataTimestamp", "8 minutes")

# mark = data_wm \
# .withColumn("media_key") \
# .join(media_wm, "media_key", "leftOuter") \
# .select("media_key", "media_type", "created_at", "text")

# user_wm = userDf.withWatermark("userTimestamp", "7 minutes").alias("A")
# data_wm = dataDf.withWatermark("dataTimestamp", "8 minutes").alias("B")
# media_wm = mediaDf.withWatermark("mediaTimestamp", "8 minutes").alias("C")

# water = user_wm \
# .join(data_wm,
#   expr("""
#     B.author_id = A.users_id AND
#     B.dataTimestamp >= A.userTimestamp AND
#     B.dataTimestamp <= A.userTimestamp + interval 1 minutes
#   """),
#   "rightOuter"
# ) \
# .join(media_wm,
#   expr("""
#     B.media_key = C.media_key AND
#     B.dataTimestamp >= C.userTimestamp AND
#     B.dataTimestamp <= C.userTimestamp + interval 1 minutes
#   """),
#   "rightOuter"
# ).select("username", "dataTimestamp", "text", "media_key")


# ----------------------------------------------------------------

## Sink to HDFS
# query =  dataDf \
#     .writeStream \
#     .outputMode("append") \
#     .format("json") \
#     .option("path","hdfs://namenode:9000/srdjan/joinDataUserMedia") \
#     .option("checkpointLocation", "file:/spark-warehouse/tmp/joinDataUserMedia") \
#     .start() \
#     .awaitTermination()


## To console
# query = joinDf3 \
#     .writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .start() \
#     .awaitTermination()
##.option("truncate", "false") \
##.trigger(processingTime='2 seconds') \
# .option("numRows", 10) \


## Sink to postgresql
def foreach_batch_function(df, epoch_id):
    df.write \
    .format("jdbc") \
    .option("driver", "org.postgresql.Driver") \
    .option("url", "jdbc:postgresql://postgres:5432/postgres") \
	  .option("dbtable", "joinDataUserMedia") \
	  .option("user", "postgres") \
    .option("password", "postgres") \
    .mode("append")\
    .save()
joinDf3.writeStream.foreachBatch(foreach_batch_function).start() \
.awaitTermination()
##.outputMode("append")


# Sink on kafka topic
# query = dataDf \
#     .selectExpr("CAST(tweet_id AS STRING) AS key", "to_json(struct(*)) AS value") \
#     .writeStream \
#     .format("kafka") \
#     .outputMode("append") \
#     .option("kafka.bootstrap.servers","kafka1:19092") \
#     .option("topic","joinDataUserMedia") \
#     .option("checkpointLocation", "file:/spark-warehouse/joinDataUserMedia") \
#     .start() \
#     .awaitTermination()


## run app:
# ./spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 --jars /home/driver/postgresql-42.3.5.jar /home/stream_zadaci/joinDataUserMedia.py

## run kafka consumer
# usr/bin/kafka-console-consumer --bootstrap-server localhost:9092 --topic joinDataUserMedia --from-beginning