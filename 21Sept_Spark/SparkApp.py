from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("MySparkApp").getOrCreate()

df=spark.read.csv("input_data.csv",header=True,inferSchema=True)

df=df.filter((df["Day"].isin(["Sat","Sun"]))).select(["Day","Temperature"])

df.write.parquet("output_data_par.parquet")

# import argparse

# Define a function to parse command-line arguments

# def parse_args():
#     parser = argparse.ArgumentParser(description="Filter DataFrame by Day")
#     parser.add_argument("--input", required=True, help="Input CSV file path")
#     parser.add_argument("--output", required=True, help="Output Parquet file path")
#     parser.add_argument("--day", required=True, help="Day to filter (e.g., 'Sat', 'Sun')")
#     return parser.parse_args()

# if __name__ == "__main__":
 
#    # Parse command-line arguments
#     args = parse_args()
#     # Create a SparkSession
#     spark = SparkSession.builder.appName("FilterExample").getOrCreate()
#     # Load the CSV data from the input file

#     df = spark.read.csv(args.input, header=True, inferSchema=True)

#     df = df.filter(df['Day'] == args.day)
#     # Save the filtered DataFrame as Parquet
#     df.write.mode("overwrite").parquet(args.output)

#     # Stop the SparkSession

 
spark.stop()
