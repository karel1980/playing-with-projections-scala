import sys
from operator import add

from pyspark.sql import SparkSession

import pyspark.sql.functions as F

from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: wordcount <file>", file=sys.stderr)
        sys.exit(-1)

    spark = SparkSession.builder \
        .appName("PlayingWithProjections04") \
        .getOrCreate()

    lines = spark.read \
        .option("multiLine", True) \
        .json(sys.argv[1])

    # quiz_id -> quiz_title
    quiz_created = lines.filter(lines.type == 'QuizWasCreated')['payload.quiz_id', 'payload.quiz_title', 'timestamp']

    # game_id -> quiz_id
    game_opened = lines.filter(lines.type == 'GameWasOpened')['payload.game_id','payload.quiz_id']

    # game_id
    game_started = lines.filter(lines.type == 'GameWasStarted')['payload.game_id', 'timestamp']
    game_started = game_started.withColumn('year_month', F.date_format(F.to_timestamp('timestamp'), 'yyyy-MM')).groupBy('year_month', 'game_id').count()

    quiz_start_counts = game_started.join(game_opened, 'game_id').groupBy(['year_month', 'quiz_id']).count()

    windowMonth = Window.partitionBy("year_month").orderBy(col("count").desc())

    top_quizes_per_month = quiz_start_counts.withColumn("row",row_number().over(windowMonth)) \
        .filter(col("row") <= 10) \
        .join(quiz_created, 'quiz_id') \
        .orderBy(col("year_month").desc()) \
        .select('year_month', 'quiz_title', 'count')

    top_quizes_per_month.show(1000, False)

    # Alternative representation, really grouped by year_month
    #top_quizes_per_month.select( \
    #    'year_month', \
    #    F.create_map(F.lit('title'), 'quiz_title', F.lit('count'), 'count').alias('quiz')) \
    #    .groupBy('year_month').agg(F.collect_list('quiz')).show(1000, False)

    spark.stop()
