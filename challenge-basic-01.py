import sys
from operator import add

from pyspark.sql import SparkSession


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: wordcount <file>", file=sys.stderr)
        sys.exit(-1)

    spark = SparkSession.builder \
        .appName("PlayingWithProjections-basic-01") \
        .getOrCreate()

    lines = spark.read \
        .option("multiLine", True) \
        .json(sys.argv[1])

    print("number of events:", lines.count())

    spark.stop()
