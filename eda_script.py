from pyspark.sql import SparkSession
import pandas as pd

# Initialize Spark session
spark = SparkSession.builder.appName("NetflixEDA").getOrCreate()

# Load Netflix dataset
data = spark.read.csv("netflix_titles.csv", header=True, inferSchema=True)

# Perform basic EDA
print("Dataset Schema:")
data.printSchema()

print("Total Records:", data.count())

# Grouping data
print("Top 10 Genres:")
data.groupBy("type").count().show(10)

# Convert to Pandas for plotting
pandas_df = data.toPandas()

# Plotting
import matplotlib.pyplot as plt
import seaborn as sns

sns.countplot(data=pandas_df, x="type")
plt.title("Content Type Distribution")
plt.show()

# Stop Spark
spark.stop()
