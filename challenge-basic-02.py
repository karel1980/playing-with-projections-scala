import sys
from operator import add

from pyspark.sql import SparkSession


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: wordcount <file>", file=sys.stderr)
        sys.exit(-1)

    spark = SparkSession.builder \
        .appName("PlayingWithProjections-basic-02") \
        .getOrCreate()

    lines = spark.read \
        .option("multiLine", True) \
        .json(sys.argv[1])

    registered = lines.filter(lines.type == 'PlayerHasRegistered')

    print("number of registerd players:", registered.count())

    spark.stop()
