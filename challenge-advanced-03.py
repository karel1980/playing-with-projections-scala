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
        .appName("PlayingWithProjections-advanced-03") \
        .getOrCreate()

    lines = spark.read \
        .option("multiLine", True) \
        .json(sys.argv[1])

    # quiz_id -> quiz_title
    quiz_created = lines.filter(lines.type == 'QuizWasCreated')['payload.quiz_id', 'payload.quiz_title']

    # game_id -> quiz_id
    game_opened = lines.filter(lines.type == 'GameWasOpened')['payload.game_id','payload.quiz_id']

    # game_id
    player_joined = lines.filter(lines.type == 'PlayerJoinedGame')['payload.game_id', 'timestamp'].select(
        'game_id', F.date_format(F.to_timestamp('timestamp'), 'yyyy-MM').alias('year_month'))

    top_quizes = player_joined \
        .join(game_opened, 'game_id') \
        .groupBy('quiz_id').count() \
        .join(quiz_created, 'quiz_id') \
        .orderBy(F.col('count').desc()) \
        .select('quiz_title','count')

    top_quizes.show()

    windowMonth = Window.partitionBy("year_month").orderBy(col("count").desc())

    top_quizes_by_month = player_joined.groupBy('year_month', 'game_id').count() \
        .join(game_opened, 'game_id') \
        .withColumn("row", row_number().over(windowMonth)) \
        .filter(col("row") <= 10) \
        .join(quiz_created, 'quiz_id') \
        .select('year_month','quiz_title','count')

    top_quizes_by_month.show()

    spark.stop()
