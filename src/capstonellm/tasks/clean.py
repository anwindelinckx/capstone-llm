import argparse
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as sf
from capstonellm.common.catalog import llm_bucket
from capstonellm.common.spark import ClosableSparkSession

logger = logging.getLogger(__name__)

def clean(spark: SparkSession, environment: str, tag: str):
    #input_path = "//raw/"
    #output_path =
    #input_path = f"s3a://{llm_bucket}/raw/{tag}"
    #output_path = f"s3a://{llm_bucket}/cleaned/{tag}"

    #logger.info(f"Reading raw data from {input_path}")
    df_answers = spark.read.json("./raw/answers.json")
    df_questions = spark.read.json("./raw/questions.json")
    #df_answers.printSchema()
    #df_questions.printSchema()

    cleaned_df_questions = df_questions.select(sf.explode('items').alias('items')).select(
         "items.question_id",
         "items.title",
         "items.body"
     ).withColumnRenamed("title", "question_title").withColumnRenamed("body", "question_body")


    cleaned_df_answers = df_answers.select(sf.explode('items').alias('items')).select(
        "items.question_id",
        "items.answer_id",
        "items.body"
    ).withColumnRenamed("question_id", "question_id").withColumnRenamed("answer_id", "answer_id").withColumnRenamed("body", "answer_body")

    #cleaned_df_answers.show()
    #cleaned_df_questions.show()

    df_joined = cleaned_df_questions.join(cleaned_df_answers, on="question_id", how="left")
    df_joined.show()

    # #logger.info(f"Writing cleaned data to {output_path}")
    df_joined.write.mode("overwrite").json("./cleaned/combined.json")


def main():
    parser = argparse.ArgumentParser(description="capstone_llm")
    parser.add_argument(
        "-e", "--env", dest="env", help="environment we are executing in", required=False, default="local"
    )
    parser.add_argument(
        "-t", "--tag", dest="tag", help="the tag to process",
        default="python-polars", required=False
    )
    logger.info("starting the cleaning job")

    args = parser.parse_args()
    common_spark_config = {
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
    }
    if args.env == "local":
        print("This is a local execution of the capestonellm project")
        session = (
            SparkSession.builder.appName("Spark S3 Integration")
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4")
            .getOrCreate()
        )
        clean(session, args.env, args.tag)
    else:
        with ClosableSparkSession("capstone_llm", spark_config=common_spark_config) as session:
            clean(session, args.env, args.tag)


if __name__ == "__main__":
    main()
