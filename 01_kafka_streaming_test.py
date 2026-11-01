# Databricks notebook source
import json

kafka_connection_json = dbutils.secrets.get(scope="finguard-scope",key = "kafka_connection_details")
kafka_config = json.loads(kafka_connection_json)
api_key = kafka_config['api_key']
api_secret = kafka_config['api_secret']
topic = kafka_config['topic']
bootstrap_servers = kafka_config['bootstrap_servers']

# COMMAND ----------

# kafka_config

# COMMAND ----------



jaas_config = f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{api_key}" password="{api_secret}";'
#   ^
#   | 
# jaas -> java authentication and authorization service

# COMMAND ----------

# MAGIC %md
# MAGIC ## Loading in batches

# COMMAND ----------


sample_batch = (spark.read.format('kafka')
                .option("kafka.bootstrap.servers", bootstrap_servers) 
                .option("subscribe", topic)
                .option("kafka.security.protocol", "SASL_SSL")        
                .option("kafka.sasl.mechanism", "PLAIN")
                .option("kafka.sasl.jaas.config", jaas_config)
                .option("startingOffsets", "earliest")                
                .load()
                )

# COMMAND ----------

sample_batch.count()

# COMMAND ----------

display(sample_batch)

# COMMAND ----------

from pyspark.sql.functions import col

parsed_batch = sample_batch.select(
    col("key").cast("string"),
    col("value").cast("string"),
    col("topic"),
    col("partition"),
    col("offset"),
    col("timestamp"),
    col("timestampType")
)

# COMMAND ----------

display(parsed_batch)

# COMMAND ----------

parsed_batch.write.saveAsTable("finguard.bronze.transactions_batch_test")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Loading in streaming data

# COMMAND ----------


streaming_df = (spark.readStream.format('kafka')
                .option("kafka.bootstrap.servers", bootstrap_servers) 
                .option("subscribe", topic)
                .option("kafka.security.protocol", "SASL_SSL")        
                .option("kafka.sasl.mechanism", "PLAIN")
                .option("kafka.sasl.jaas.config", jaas_config)
                .option("startingOffsets", "earliest")                
                .load()
                )

# COMMAND ----------

from pyspark.sql.functions import col

parsed_streaming_df = streaming_df.select(
    col("key").cast("string"),
    col("value").cast("string"),
    col("topic"),
    col("partition"),
    col("offset"),
    col("timestamp"),
    col("timestampType")
)

# COMMAND ----------

## run this if there is new data, not above cells
streaming_query = (parsed_streaming_df.writeStream
    .format("delta")
    .outputMode("append")  
    .option("checkpointLocation", "/Volumes/finguard/source/transactions/checkpoint/") 
    .trigger(availableNow=True)
    .toTable("finguard.bronze.transactions_streaming_test") 
)

# COMMAND ----------

print("Query started with query id: ", streaming_query.id)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from finguard.bronze.transactions_streaming_test ;

# COMMAND ----------

