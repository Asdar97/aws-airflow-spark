import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum, avg, count, datediff, current_date, to_timestamp

def ewallet_transaction_analysis(input_loc, output_loc):
    """
    This function performs analysis on e-wallet transaction data:
    1. Cleans and transforms input data
    2. Performs various aggregations and calculations
    3. Writes results to HDFS output
    """
    spark = SparkSession.builder.appName("E-wallet Transaction Analysis").getOrCreate()

    # Read input as string for timestamp fields
    df_raw = spark.read.option("header", True).csv(input_loc)
    df_raw = df_raw.withColumn("DateStr", col("Date").cast("string"))

    # Convert string to timestamp
    df_clean = df_raw.withColumn("Date", to_timestamp(col("DateStr"), "yyyy-MM-dd HH:mm:ss.SSS"))

    # Clean and transform data
    df_clean = df_clean.withColumn("Amount", col("Amount").cast("double")) \
                       .withColumn("Balance_Before", col("Balance_Before").cast("double")) \
                       .withColumn("Balance_After", col("Balance_After").cast("double")) \
                       .withColumn("Date", col("Date").cast("date"))

    # Add a column for transaction age in days
    df_clean = df_clean.withColumn("TransactionAgeInDays", datediff(current_date(), col("Date")))

    # Perform aggregations and calculations
    df_user_summary = df_clean.groupBy("User_ID").agg(
        count("Transaction_ID").alias("TotalTransactions"),
        sum(when(col("Transaction_Type") == "Payment", col("Amount")).otherwise(0)).alias("TotalSpent"),
        avg("Amount").alias("AverageTransactionAmount"),
        sum(when(col("Transaction_Type") == "Top-up", col("Amount")).otherwise(0)).alias("TotalTopUp")
    )

    df_category_summary = df_clean.filter(col("Transaction_Type") == "Payment") \
                                  .groupBy("Purchase_Category") \
                                  .agg(sum("Amount").alias("TotalAmount"),
                                       count("Transaction_ID").alias("TransactionCount"))

    # Identify high-value customers (example threshold: more than 10 transactions and average transaction amount > 100)
    df_high_value_customers = df_user_summary.filter((col("TotalTransactions") > 10) & (col("AverageTransactionAmount") > 100))

    # Combine results
    df_out = df_user_summary.join(df_high_value_customers, ["User_ID"], "left_outer") \
                            .select(
                                df_user_summary["*"],  # Select all columns from df_user_summary
                                # Add any specific columns from df_high_value_customers if needed, ensuring they are aliased if they exist in df_user_summary
                                when(df_high_value_customers["User_ID"].isNotNull(), True).otherwise(False).alias("IsHighValueCustomer")
                            )

    # Write results
    df_clean.write.mode("overwrite").parquet(output_loc + "/clean_data")
    df_out.write.mode("overwrite").parquet(output_loc + "/user_summary")
    df_category_summary.write.mode("overwrite").parquet(output_loc + "/category_summary")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=str, help="HDFS input", default="/input")
    parser.add_argument("--output", type=str, help="HDFS output", default="/output")
    args = parser.parse_args()

    ewallet_transaction_analysis(input_loc=args.input, output_loc=args.output)