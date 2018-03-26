from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions

def parseInput(line):
    fields = line.split("\t")
    # In Python 3, long is renamed to int
    return Row(user_id = int(fields[0]), movie_id = int(fields[1]), rating = int(fields[2]), rating_ts = long(fields[3]))

if __name__ == "__main__":
    # Create a SparkSession
    spark = SparkSession.builder.appName("MongoDBIntegration").getOrCreate()

    # Get the raw data
    lines = spark.sparkContext.textFile("hdfs:///user/maria_dev/ml-100k/u.data")
    # Convert it to a RDD of Row objects with (userID, movieID, rating, ratingTs)
    ratings = lines.map(parseInput)
    # Convert that to a DataFrame
    ratingsDataset = spark.createDataFrame(ratings)

    # Write it into MongoDB
    ratingsDataset.write\
        .format("com.mongodb.spark.sql.DefaultSource")\
        .option("uri","mongodb://127.0.0.1/movielens.ratings")\
        .mode('append')\
        .save()

    # Read it back from MongoDB into a new Dataframe
    readUsers = spark.read\
    .format("com.mongodb.spark.sql.DefaultSource")\
    .option("uri","mongodb://127.0.0.1/movielens.ratings")\
    .load()

    readUsers.createOrReplaceTempView("ratings")

    sqlDF = spark.sql("SELECT * FROM ratings WHERE rating < 3")
    sqlDF.show()

    # Stop the session
    spark.stop()
