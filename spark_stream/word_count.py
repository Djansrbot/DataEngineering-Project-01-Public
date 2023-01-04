from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def quiet_logs(sc):
  logger = sc._jvm.org.apache.log4j
  logger.LogManager.getLogger("org"). setLevel(logger.Level.ERROR)
  logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

spark = SparkSession \
    .builder \
    .appName("wordCount") \
    .getOrCreate()

quiet_logs(spark)

# ----------------------------------------------------------------
#Data Stream
dataStream = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "kafka1:19092") \
  .option("subscribe", "dataStream01") \
  .load()
 # .option("startingOffsets", "earliest") \

df = dataStream.selectExpr("CAST(key AS STRING)","CAST(value AS STRING)","CAST(timestamp AS TIMESTAMP)")

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
        StructField('media_keys', ArrayType(StringType(), True)
  )])),
  StructField('id', StringType(), True),
  StructField('created_at', TimestampType(), True),
  StructField('text', StringType(), True)
])

# ----------------------------------------------------------------

#data columns
dataDf = df.select(from_json(col("value"), dataSchema).alias("data"), col("timestamp")) \
.select(
  col("data.id").alias("tweet_id"),
  col("data.created_at").alias("created_at"),
  col("data.text").alias("text"),
  col("data.public_metrics.retweet_count").cast(IntegerType()).alias("retweet_count"),
  col("data.public_metrics.reply_count").cast(IntegerType()).alias("reply_count"),
  col("data.public_metrics.like_count").cast(IntegerType()).alias("like_count"),
  col("data.public_metrics.quote_count").cast(IntegerType()).alias("quote_count"),
  col("data.author_id").alias("author_id"),
  col("data.attachments.media_keys").alias("data_media_keys"),
  col("timestamp").alias("wcTimestamp")
).withColumn("media_key", explode_outer("data_media_keys")).drop("data_media_keys")
## .withColumn("datetype", to_date(col("created_at"),"yyyy-MM-dd"))

# ----------------------------------------------------------------


# App most used words
stopWordList = ["i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you", "your", "yours", "yourself",\
                                                        "yourselves", "he", "him", "his", "himself", "she", "her", "hers", "herself", "it", "its", "itself",\
                                                        "they", "them", "their", "theirs", "themselves", "what", "which", "who", "whom", "this", "that", "these",\
                                                        "those", "am", "is", "are", "was", "were", "be", "been", "being", "have", "has", "had", "having", "do", "does",\
                                                        "did", "doing", "a", "an", "the", "and", "but", "if", "or", "because", "as", "until", "while", "of", "at", "by",\
                                                        "for", "with", "about", "against", "between", "into", "through", "during", "before", "after", "above", "below",\
                                                        "to", "from", "up", "down", "in", "out", "on", "off", "over", "under", "again", "further", "then", "once", "here",\
                                                        "there", "when", "where", "why", "how", "all", "any", "both", "each", "few", "more", "most", "other", "some", "such",\
                                                        "no", "nor", "not", "only", "own", "same", "so", "than", "too", "very", "s", "t", "can", "will", "just", "don",\
                                                        "should", "now", "says", "new", "us", "say", "said", "one", "two", "—", "-", "+", "--", ".", ",", "=", "...", ":", "@", '"', ":"]

# wordCount
# wordsArray = dataDf \
# .select("created_at", split(col("text")," ").alias("words"))

# wordCount = wordsArray \
# .select("created_at" ,explode("words").alias("word")) \
# .withColumn("word", lower(translate("word", ".:()@", ""))) \
# .filter(~lower(col("word")).isin(stopWordList)) \
# .withWatermark("created_at", "30 seconds") \
# .groupBy("word", window("created_at","25 seconds", "5 seconds")).count()

words = dataDf \
.select("created_at", explode(split(col("text"), " ")).alias("word")) \
.filter(~lower(col("word")).isin(stopWordList)) \
.withWatermark("created_at", "30 seconds")

wordCounts = words.groupBy("word", window("created_at","25 seconds", "5 seconds")).count()

# wordCount2 = wordCount \
# .withWatermark("created_at", "1 minutes") \
# .withColumn("window", window("created_at","1 minutes", "30 seconds"))

# df.withColumn("datetype",
#     to_date(col("input_timestamp"),"yyyy-MM-dd"))

# ------------------------------------------------------------------------------------

#HDFS
# query = wordCount2 \
#     .writeStream \
#     .outputMode("append") \
#     .format("json") \
#     .option("path","hdfs://namenode:9000/srdjan/wordCount") \
#     .option("checkpointLocation", "file:/spark-warehouse/tmp/wordCount") \
#     .start() \
#     .awaitTermination()


query = wordCounts \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start() \
    .awaitTermination()
##.option("truncate", "false") \
##.trigger(processingTime='2 seconds') \


# def foreach_batch_function(df, epoch_id):
#     df.write \
#     .format("jdbc") \
#     .option("driver", "org.postgresql.Driver") \
#     .option("url", "jdbc:postgresql://postgres:5432/postgres") \
# 	  .option("dbtable", "wordCount") \
# 	  .option("user", "postgres") \
#     .option("password", "postgres") \
#     .mode("append")\
#     .save()
# wordCounts.writeStream.outputMode("append").foreachBatch(foreach_batch_function).start() \
# .awaitTermination()
##.outputMode("append")


# Na kafka topic
# query = words_count \
#     .selectExpr("CAST(word AS STRING) AS key", "CAST(count AS STRING) AS value") \
#     .writeStream \
#     .format("kafka") \
#     .outputMode("complete") \
#     .option("kafka.bootstrap.servers","kafka1:19092") \
#     .option("topic","wordCount") \
#     .option("checkpointLocation", "file:/spark-warehouse/wordCount") \
#     .start() \
#     .awaitTermination()



## Run app:
# ./spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 --jars /home/driver/postgresql-42.3.5.jar /home/stream_zadaci/word_count.py

## run kafka consumer
# usr/bin/kafka-console-consumer --bootstrap-server localhost:9092 --topic wordCount --from-beginning