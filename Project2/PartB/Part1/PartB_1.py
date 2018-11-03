import pyspark.sql.functions as func
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext

my_spark = SparkSession.builder.getOrCreate()

df = my_spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri", "mongodb://student:student@ec2-54-210-44-189.compute-1.amazonaws.com/test.reviews").load()
df1 = my_spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri", "mongodb://student:student@ec2-54-210-44-189.compute-1.amazonaws.com/test.metadata").load()


df3 = df.groupBy("asin").agg(func.sum("overall").alias("item_sum"), func.count(func.lit(1)).alias("item_counts"))
df3 = df3.filter(df3["item_counts"] >= 100)
df3 = df3.withColumn("item_avg", func.col("item_sum") / func.col("item_counts")).drop('item_sum')

#select from metadata
df4 = df1.select("asin","categories","title")
df5 = df4.join(df3,"asin","inner").drop("asin")

df6 = df5.select("categories","title","item_counts","item_avg", func.explode_outer("categories"))
df7 = df6.drop("categories").withColumnRenamed("col","categories")
df9 = df7.select("categories","title","item_counts","item_avg").sort("categories")

#df9 = df8.collect()
file = open("PartB/Part1/output.txt", "w")

cdsVinyl = df9.filter(df9["categories"] == "CDs & Vinyl").sort("item_avg", ascending=False).limit(1)
moviesTv = df9.filter(df9["categories"] == "Movies & TV").sort("item_avg", ascending=False).limit(1)
toysGame = df9.filter(df9["categories"] == "Toys & Games").sort("item_avg", ascending=False).limit(1)
videoGam = df9.filter(df9["categories"] == "Video Games").sort("item_avg", ascending=False).limit(1)
#############################################
df10 = cdsVinyl.collect()
output = map(lambda x : "%s\t%s\t%d\t%f" % (x.categories, x.title, x.item_counts, x.item_avg), df10)
file.write(output[0] + '\n')
##############################################
df11 = moviesTv.collect()
output = map(lambda x : "%s\t%s\t%d\t%f" % (x.categories, x.title, x.item_counts, x.item_avg), df11)
file.write(output[0] + '\n')
##############################################
df12 = toysGame.collect()
output = map(lambda x : "%s\t%s\t%d\t%f" % (x.categories, x.title, x.item_counts, x.item_avg), df12)
file.write(output[0] + '\n')
##############################################
df13 = videoGam.collect()
output = map(lambda x : "%s\t%s\t%d\t%f" % (x.categories, x.title, x.item_counts, x.item_avg), df13)
file.write(output[0] + '\n')
#############################################
file.close()

