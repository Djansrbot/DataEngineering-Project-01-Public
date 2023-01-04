import os
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import *
from pyspark.sql.types import *
import re
HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]

def quiet_logs(sc):
  logger = sc._jvm.org.apache.log4j
  logger.LogManager.getLogger("org"). setLevel(logger.Level.ERROR)
  logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

spark = SparkSession \
    .builder \
    .appName("cleanData") \
    .getOrCreate()

quiet_logs(spark)


df=spark.read.format("avro").load(HDFS_NAMENODE + "/out/")
df.printSchema()

#Podaci koji mi trebaju
data = df.select(explode("data").alias("data")) \
.select(
  col("data.id").alias("tweet_id"),
  col("data.created_at").cast(TimestampType()).alias("created_at"),
  col("data.text").alias("text"),
  col("data.public_metrics.retweet_count").cast(IntegerType()).alias("retweet_count"),
  col("data.public_metrics.reply_count").cast(IntegerType()).alias("reply_count"),
  col("data.public_metrics.like_count").cast(IntegerType()).alias("like_count"),
  col("data.public_metrics.quote_count").cast(IntegerType()).alias("quote_count"),
  col("data.author_id").alias("author_id"),
  col("data.attachments.media_keys").alias("data_media_keys")
)
#data.orderBy("tweet_id").show()
data.printSchema()

includesMedia = df.select(explode("includes.media").alias("media")) \
.select( 
  col("media.media_key").alias("media_key"),
  col("media.type").alias("media_type")
)
#includesMedia.orderBy("media_key").show()
#includesMedia.printSchema()

includesUsers = df.select(explode("includes.users").alias("users")) \
.select( 
  col("users.username").alias("username"),
  col("users.description").alias("description"),
  col("users.id").alias("users_id"),
  col("users.name").alias("name")
).distinct()
#includesUsers.show()
includesUsers.printSchema()

#suvišno
#photo,video,gif count group by tweet_id
df1 = data.select("tweet_id", explode("data_media_keys").alias("media_key")) \
.join(includesMedia, "media_key", "outer") \
.groupBy("tweet_id") \
.agg(
  count(when(col("media_type") == 'photo', '1')).alias("photo_count"),
  count(when(col("media_type") == 'video', '1')).alias("video_count"),
  count(when(col("media_type") == 'animated_gif', '1')).alias("gif_count")
)
#.show()

#suvišno
# photo,video,gif count group by tweet_id, username, text, 
df2 = data.select("tweet_id","text", "author_id", explode("data_media_keys").alias("media_key")) \
.join(includesMedia, "media_key", "outer") \
.join(includesUsers, col("author_id") == col("users_id"), "outer") \
.groupBy("tweet_id","username", "text") \
.agg(
  count(when(col("media_type") == 'photo', '1')).alias("photo_count"),
  count(when(col("media_type") == 'video', '1')).alias("video_count"),
  count(when(col("media_type") == 'animated_gif', '1')).alias("gif_count")
)
#.show()

# metabase Media
# photo,video,gif count group by name
df3 = data.select("author_id","tweet_id", explode("data_media_keys").alias("media_key")) \
.join(includesMedia, "media_key", "outer") \
.join(includesUsers, col("author_id") == col("users_id"), "outer") \
.groupBy("username") \
.agg(
  count(when(col("media_type") == 'photo', '1')).alias("photo_count"),
  count(when(col("media_type") == 'video', '1')).alias("video_count"),
  count(when(col("media_type") == 'animated_gif', '1')).alias("gif_count")
)
#.show()

#metabase Tweets per day
# tweet_id, created_at, username, text, retweet, reply, like, quote count ordered by created_at
df4 = data.join(includesUsers, col("author_id") == col("users_id"), "outer") \
.select("tweet_id", "created_at", "username", "text", "retweet_count", "reply_count", "like_count", "quote_count") \
.orderBy("created_at")
#.show()

#metabase Tweet interaction
# sum retweet, reply, like, quote count grouped by author_id, name
df5 = data.join(includesUsers, col("author_id") == col("users_id"), "outer") \
.groupBy("author_id","username").sum("retweet_count", "reply_count", "like_count", "quote_count")
#.show()

#metabase Biden or Trump
# username, text filtered by word groupBy Biden
df6 = data.join(includesUsers, col("author_id") == col("users_id"), "outer") \
.select("username", "text").filter(col("text").rlike("(?i)biden?")).groupBy("username").count() \
#.show()

#metabase Biden or Trump
# username, text filtered by word groupBy Trump
df9 = data.join(includesUsers, col("author_id") == col("users_id"), "outer") \
.select("username", "text").filter(col("text").rlike("(?i)trump?")).groupBy("username").count() \
#.show()

#sve kolone
df_join1 = data.select("tweet_id", "author_id", "created_at", "text", "retweet_count", "reply_count", "like_count", "quote_count", explode("data_media_keys").alias("media_key")) \
.join(includesMedia, "media_key", "outer")
df7 = df_join1.join(includesUsers, col("author_id") == col("users_id"), "outer") \
.select("tweet_id", "created_at", "username", "text", "retweet_count", "reply_count", "like_count", "quote_count", "media_key", "media_type", "author_id", "name", "description")
#.show(5)


#metabase Top 10 words
stopWordList = ["i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you", "your", "yours", "yourself",\
                                                        "yourselves", "he", "him", "his", "himself", "she", "her", "hers", "herself", "it", "its", "itself",\
                                                        "they", "them", "their", "theirs", "themselves", "what", "which", "who", "whom", "this", "that", "these",\
                                                        "those", "am", "is", "are", "was", "were", "be", "been", "being", "have", "has", "had", "having", "do", "does",\
                                                        "did", "doing", "a", "an", "the", "and", "but", "if", "or", "because", "as", "until", "while", "of", "at", "by",\
                                                        "for", "with", "about", "against", "between", "into", "through", "during", "before", "after", "above", "below",\
                                                        "to", "from", "up", "down", "in", "out", "on", "off", "over", "under", "again", "further", "then", "once", "here",\
                                                        "there", "when", "where", "why", "how", "all", "any", "both", "each", "few", "more", "most", "other", "some", "such",\
                                                        "no", "nor", "not", "only", "own", "same", "so", "than", "too", "very", "s", "t", "can", "will", "just", "don",\
                                                        "should", "now", "says", "new", "us", "say", "said", "one", "two", "—", "-", "+", "--", ".", ",", "=", "...", ":", "@"]

wordsArray = data.select(split(col("text")," ").alias("words"))
df8 = wordsArray.select(explode("words").alias("word")) \
.withColumn("word", lower(translate("word", ".:()@", ""))) \
.filter(~lower(col("word")).isin(stopWordList)) \
.groupBy("word").count().orderBy(desc("count"))




# print((data.count(), len(data.columns)))


#test = data.join(includesUsers, col("author_id") == col("users_id"), "outer").show(10)
#test.filter(col("username") == "breakingweather").show(10)
#test.filter(col("author_id") == "142614009").show(10)


# tempData\
# .select("text","like_count","author_id") \
# .withColumn("acuWeather","19071682").show(2)


#Agregirani podaci
# tempData.groupBy("author_id") \
#   .agg(
#     sum(tempData["like_count"]).alias("sum_likeCount"),
#     avg(col("like_count")).alias("avg_likeCount")
#   ).show()


#df.agg(sum("like_count")).show()


#Pisanje u postgres
# df1.write \
#     .format("jdbc") \
#     .mode("overwrite") \
#     .option("driver", "org.postgresql.Driver") \
#     .option("url", "jdbc:postgresql://postgres:5432/postgres") \
# 	  .option("dbtable", "df1") \
# 	  .option("user", "postgres") \
#     .option("password", "postgres") \
#     .save()

# df2.write \
#     .format("jdbc") \
#     .mode("overwrite") \
#     .option("driver", "org.postgresql.Driver") \
#     .option("url", "jdbc:postgresql://postgres:5432/postgres") \
# 	  .option("dbtable", "df2") \
# 	  .option("user", "postgres") \
#     .option("password", "postgres") \
#     .save()

# df3.write \
#     .format("jdbc") \
#     .mode("overwrite") \
#     .option("driver", "org.postgresql.Driver") \
#     .option("url", "jdbc:postgresql://postgres:5432/postgres") \
# 	  .option("dbtable", "df3") \
# 	  .option("user", "postgres") \
#     .option("password", "postgres") \
#     .save()

# df4.write \
#     .format("jdbc") \
#     .mode("overwrite") \
#     .option("driver", "org.postgresql.Driver") \
#     .option("url", "jdbc:postgresql://postgres:5432/postgres") \
# 	  .option("dbtable", "df4") \
# 	  .option("user", "postgres") \
#     .option("password", "postgres") \
#     .save()

# df5.write \
#     .format("jdbc") \
#     .mode("overwrite") \
#     .option("driver", "org.postgresql.Driver") \
#     .option("url", "jdbc:postgresql://postgres:5432/postgres") \
# 	  .option("dbtable", "df5") \
# 	  .option("user", "postgres") \
#     .option("password", "postgres") \
#     .save()

# df6.write \
#     .format("jdbc") \
#     .mode("overwrite") \
#     .option("driver", "org.postgresql.Driver") \
#     .option("url", "jdbc:postgresql://postgres:5432/postgres") \
# 	  .option("dbtable", "df6") \
# 	  .option("user", "postgres") \
#     .option("password", "postgres") \
#     .save()

# df7.write \
#     .format("jdbc") \
#     .mode("overwrite") \
#     .option("driver", "org.postgresql.Driver") \
#     .option("url", "jdbc:postgresql://postgres:5432/postgres") \
# 	  .option("dbtable", "df7") \
# 	  .option("user", "postgres") \
#     .option("password", "postgres") \
#     .save()

# df8.write \
#   .format("jdbc") \
#   .mode("overwrite") \
#   .option("driver", "org.postgresql.Driver") \
#   .option("url", "jdbc:postgresql://postgres:5432/postgres") \
#   .option("dbtable", "df8") \
#   .option("user", "postgres") \
#   .option("password", "postgres") \
#   .save()

# df9.write \
#     .format("jdbc") \
#     .mode("overwrite") \
#     .option("driver", "org.postgresql.Driver") \
#     .option("url", "jdbc:postgresql://postgres:5432/postgres") \
# 	  .option("dbtable", "df9") \
# 	  .option("user", "postgres") \
#     .option("password", "postgres") \
#     .save()

#Pokretanje programa
# ./spark/bin/spark-submit --packages org.apache.spark:spark-avro_2.12:2.4.4 --jars /home/driver/postgresql-42.3.5.jar /home/spark-zadaci/cleanData.py 