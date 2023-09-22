from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("RDDMapExample").getOrCreate()
data=[1,2,3,4,5]
rdd=spark.sparkContext.parallelize(data)
def double(x):
	return x*2

result_rdd=rdd.map(double)
result=result_rdd.collect()
print(result)
spark.stop()
