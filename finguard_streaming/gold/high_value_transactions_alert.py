from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
import json
from pyspark.sql.functions import col
from pyspark.sql import functions as F
from pyspark.sql.types import *

@dp.table(
    name = "finguard.gold.high_value_transactions_alert",
    comment = "Alert details where transactions has been performed with higher value than that what is configured by customer"
)
def high_value_transactions_alert() -> DataFrame:
    
    transactions = spark.readStream.table("finguard.silver.transactions") # streaming table
    customers = spark.read.table("finguard.silver.customers") # static table

    ## we never do inner join with stream table and static table, we rather do left join
    joined_df=(transactions.join(customers,transactions.customer_id==customers.customer_id,"left")
                .filter(F.col("amount")>F.col("transaction_limit"))
                .select(
                    F.concat_ws("-",F.lit("ALERT"),F.col("transaction_id")).alias("alert_id"),
                    F.lit("HIGH_VALUE_TRANSACTION").alias("alert_type"),
                    F.current_timestamp().alias("alert_timestamp"),
              
                    transactions.transaction_id,
                    transactions.customer_id,
                    customers.email.alias("customer_email"),
                    F.concat_ws(" ",F.col("first_name"),F.col("last_name")).alias("customer_name"),
                    transactions.amount.alias("transaction_amount"),
                    customers.transaction_limit,
                    transactions.currency,
                    transactions.merchant_name,
                    transactions.merchant_category,
                    transactions.transaction_type,
                    transactions.payment_channel,
                    transactions.city,
                    transactions.country,
                    transactions.is_international,
                    transactions.transaction_timestamp,
                    transactions.status 
                )
            )
    return joined_df
    