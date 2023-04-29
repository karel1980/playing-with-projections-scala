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

    # question_id, answer
    question_created = lines.filter(lines.type == 'QuestionAddedToQuiz')['payload.question_id', 'payload.answer']

    # player_id, question_id, answer
    answer_given = lines.filter(lines.type == 'AnswerWasGiven')['payload.player_id','payload.question_id', col('payload.answer').alias('player_answer')]

    answer_and_question = answer_given.join(question_created)

    answer_and_question = answer_and_question.withColumn('correct', answer_and_question.player_answer == answer_and_question.answer)

    answer_and_question.show()

    # TODO this is incomplete, because none of the player's answers seem to be correct. Check with Michel

    spark.stop()
