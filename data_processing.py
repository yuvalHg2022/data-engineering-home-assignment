from pyspark.sql import SparkSession
from pyspark.sql.functions import lag, col, stddev, row_number, last, first, when
from pyspark.sql.window import Window
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext

# Initialize GlueContext and SparkSession
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Initialize Job
job = Job(glueContext)
job.init('stock_etl_analysis')


def upload_df_to_s3_as_parquet(df, bucket_name, prefix_path):
    """
    Uploads the DataFrame as Parquet files to the specified S3 bucket path.

    Args:
        df: Spark DataFrame to upload.
        bucket_name: S3 bucket name.
        prefix_path: Path within the S3 bucket to upload the files.
    """
    s3_path = f"s3://{bucket_name}/{prefix_path}"
    df.write.mode('overwrite').parquet(s3_path)
    print(f"Data successfully uploaded to {s3_path}")


def calculate_avg_daily_return(df):
    """
    Calculate the average daily return for all stocks on each date.
    """
    window_spec = Window.partitionBy("ticker").orderBy("Date")

    # Add a new column for the previous day's closing price using the lag function
    df = df.withColumn("previous_close", lag("close").over(window_spec))

    # Calculate the daily return
    df = df.withColumn("daily_return", (col("close") - col("previous_close")) / col("previous_close"))

    # Calculate the average daily return for all stocks on each date
    df_avg_return = df.groupBy("Date").agg({"daily_return": "avg"}).withColumnRenamed("avg(daily_return)", "average_return")

    # Rename columns to match desired format
    df_avg_return = df_avg_return.withColumnRenamed("Date", "date")

    return df_avg_return


def find_highest_worth_stock(df):
    """
    Find the stock with the highest average trading worth.
    """
    df = df.withColumn("worth", col("close") * col("volume"))

    # Group by ticker and calculate the average worth for each stock
    df_avg_worth = df.groupBy("ticker").agg({"worth": "avg"}).withColumnRenamed("avg(worth)", "value")

    # Select the stock with the highest average worth
    highest_avg_worth_stock = df_avg_worth.orderBy(col("value").desc()).select("ticker", "value").limit(1)

    return highest_avg_worth_stock


def find_most_volatile_stock(df):
    """
    Find the most volatile stock based on the annualized standard deviation of daily returns.
    """
    annualization_factor = 252 ** 0.5

    # First, calculate the daily return
    window_spec = Window.partitionBy("ticker").orderBy("Date")
    df = df.withColumn("previous_close", lag("close").over(window_spec))
    df = df.withColumn("daily_return", (col("close") - col("previous_close")) / col("previous_close"))

    # Calculate the standard deviation of daily returns for each stock
    df_volatile = df.groupBy("ticker").agg(stddev("daily_return").alias("std_dev"))

    # Annualize the standard deviation
    df_volatile = df_volatile.withColumn("standard_deviation", col("std_dev") * annualization_factor).select("ticker", "standard_deviation")

    # Select the most volatile stock
    most_volatile_stock = df_volatile.orderBy(col("standard_deviation").desc()).limit(1)

    return most_volatile_stock


def find_top_30_day_return_dates(df):
    """
    Find the top three dates with the highest 30-day returns.
    """
    window_spec_30 = Window.partitionBy("ticker").orderBy("Date")

    # Get the closing price from 30 days ago using the lag function
    df = df.withColumn("close_30_days_ago", lag("close", 30).over(window_spec_30))

    # Calculate the 30-day return
    df = df.withColumn("30_day_return", (col("close") - col("close_30_days_ago")) / col("close_30_days_ago"))

    # Get the top 3 dates with the highest 30-day returns
    window_spec_rank = Window.orderBy(col("30_day_return").desc())
    top_3_return_30_dates_df = (
        df.withColumn("rank", row_number().over(window_spec_rank))
        .filter(col("rank") <= 3)
        .select("ticker", "Date")
        .withColumnRenamed("Date", "date")
    )

    return top_3_return_30_dates_df


def main():
    # Load the CSV file into a DataFrame
    file_path = 's3://data-engineer-assignment-yuval-huga/data/stocks_data.csv'
    df = spark.read.option("header", "true").csv(file_path)

    # S3 Bucket name
    bucket_name = "data-engineer-assignment-yuval-huga"

    # Step 1: Calculate average daily returns
    avg_daily_returns = calculate_avg_daily_return(df)
    upload_df_to_s3_as_parquet(avg_daily_returns, bucket_name, "output/calculate_avg_daily_return")

    # Step 2: Find the highest worth stock
    highest_worth_stock = find_highest_worth_stock(df)
    upload_df_to_s3_as_parquet(highest_worth_stock, bucket_name, "output/highest_worth_stock")

    # Step 3: Find the most volatile stock
    most_volatile_stock = find_most_volatile_stock(df)
    upload_df_to_s3_as_parquet(most_volatile_stock, bucket_name, "output/most_volatile_stock")

    # Step 4: Find the top 3 dates with the highest 30-day returns
    top_3_return_dates = find_top_30_day_return_dates(df)
    upload_df_to_s3_as_parquet(top_3_return_dates, bucket_name, "output/top_30_day_return_dates")


if __name__ == "__main__":
    main()
