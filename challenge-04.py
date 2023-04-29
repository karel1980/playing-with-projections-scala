import sys
from operator import add

from pyspark.sql import SparkSession

import pyspark.sql.functions as F


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
    quiz_created = lines.filter(lines.type == 'QuizWasCreated')['payload.quiz_id', 'payload.quiz_title']

    # game_id -> quiz_id
    game_opened = lines.filter(lines.type == 'GameWasOpened')['payload.game_id','payload.quiz_id']

    # game_id
    game_started = lines.filter(lines.type == 'GameWasStarted')[['payload.game_id']]

    print("quiz created", quiz_created.count())
    print("game opened", game_opened.count())
    print("game started", game_started.count())

    quiz_start_counts = game_started.join(game_opened, 'game_id').groupBy('quiz_id').count()
    
    top_quizes = quiz_start_counts.orderBy(F.col('count').desc()).limit(10)

    # extra count because join does not preserve order
    top_quizes_with_title = top_quizes.join(quiz_created, 'quiz_id').orderBy(F.col('count').desc())

    for quiz in top_quizes_with_title.collect():
        print("top quiz", quiz)

    spark.stop()
