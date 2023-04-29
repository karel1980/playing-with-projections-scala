import sys
from operator import add

from pyspark.sql import SparkSession

import pyspark.sql.functions as F


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: wordcount <file>", file=sys.stderr)
        sys.exit(-1)

    spark = SparkSession.builder \
        .appName("PlayingWithProjections-basic-03") \
        .getOrCreate()

    lines = spark.read \
        .option("multiLine", True) \
        .json(sys.argv[1])

    registered = lines.filter(lines.type == 'PlayerHasRegistered')

    by_year_month = registered.withColumn('year_month', F.date_format(F.to_timestamp('timestamp'), 'yyyy-MM')).groupBy('year_month').count()

    print("number of registerd players by month:", by_year_month.collect())

    spark.stop()
