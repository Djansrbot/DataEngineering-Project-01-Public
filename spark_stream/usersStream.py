from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def quiet_logs(sc):
  logger = sc._jvm.org.apache.log4j
  logger.LogManager.getLogger("org"). setLevel(logger.Level.ERROR)
  logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

spark = SparkSession \
    .builder \
    .appName("usersStream") \
    .getOrCreate()

quiet_logs(spark)

# ----------------------------------------------------------------

# users schema
usersSchema = StructType([
    StructField('description', StringType(), True),
    StructField('username', StringType(), True),
    StructField('name', StringType(), True),
    StructField('id', StringType(), True)
])

# ----------------------------------------------------------------

usersStream = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "kafka1:19092") \
  .option("subscribe", "usersStream1") \
  .load()
## .option("startingOffsets", "earliest") \

df1 = usersStream.selectExpr("CAST(key AS STRING)","CAST(value AS STRING)")

# --------------------------------------------------------------------------------------------

#user columns
userDf= df1.select(from_json(col("value"), usersSchema).alias("users")) \
.select( 
  col("users.username").alias("username"),
  col("users.description").alias("description"),
  col("users.id").alias("users_id"),
  col("users.name").alias("name")
).distinct()

# ------------------------------------------------------------------------------------

## Sink to HDFS
# query =  userDf \
#     .writeStream \
#     .outputMode("append") \
#     .format("json") \
#     .option("path","hdfs://namenode:9000/srdjan/usersStream") \
#     .option("checkpointLocation", "file:/spark-warehouse/tmp/usersStream") \
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
def foreach_batch_function(df, epoch_id):
    df.write \
    .format("jdbc") \
    .option("driver", "org.postgresql.Driver") \
    .option("url", "jdbc:postgresql://postgres:5432/postgres") \
	  .option("dbtable", "usersStream") \
	  .option("user", "postgres") \
    .option("password", "postgres") \
    .mode("append")\
    .save()
userDf.writeStream.foreachBatch(foreach_batch_function).start() \
.awaitTermination()
##.outputMode("append")


# Sink on kafka topic
# query = userDf \
#     .selectExpr("CAST(users_id AS STRING) AS key", "to_json(struct(*)) AS value") \
#     .writeStream \
#     .format("kafka") \
#     .outputMode("append") \
#     .option("failOnDataLoss", "false") \
#     .option("kafka.bootstrap.servers","kafka1:19092") \
#     .option("topic","usersStream") \
#     .option("checkpointLocation", "file:/spark-warehouse/usersStream") \
#     .start() \
#     .awaitTermination()


## run app:
# ./spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 --jars /home/driver/postgresql-42.3.5.jar /home/stream_zadaci/usersStream.py

## run kafka consumer
# usr/bin/kafka-console-consumer --bootstrap-server localhost:9092 --topic usersStream --from-beginning