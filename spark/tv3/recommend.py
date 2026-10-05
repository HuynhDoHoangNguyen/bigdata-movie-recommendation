"""CLI for on-demand or batch Top-N generation from the saved TV3 model."""

import argparse

from pyspark.ml.recommendation import ALSModel
from pyspark.sql import SparkSession

from config import (
    MODEL_PATH,
    MOVIES_PATH,
    RECOMMENDATION_CANDIDATE_MULTIPLIER,
    RECOMMENDATIONS_PATH,
    TOP_N,
    TRAIN_PATH,
)
from topn import recommend_unseen
from output_contract import validate_recommendation_path


def parse_args():
    parser = argparse.ArgumentParser(description="Generate unseen MovieLens recommendations")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--user-id", type=int)
    group.add_argument("--all-users", action="store_true")
    group.add_argument("--users-file", help="Parquet path containing an integer userId column")
    parser.add_argument("--top-n", type=int, default=TOP_N)
    parser.add_argument("--output", default=RECOMMENDATIONS_PATH)
    return parser.parse_args()


def main():
    args = parse_args()
    validate_recommendation_path(args.output)
    if args.top_n <= 0:
        raise ValueError("--top-n must be positive")
    spark = SparkSession.builder.appName("MovieLensTV3Recommend").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    train = spark.read.parquet(TRAIN_PATH).select("userId", "movieId")
    movies = spark.read.parquet(MOVIES_PATH).select("movieId", "title", "genres")
    model = ALSModel.load(MODEL_PATH)
    if args.user_id is not None:
        users = spark.createDataFrame([(args.user_id,)], "userId INT")
        if not model.userFactors.where(model.userFactors.id == args.user_id).limit(1).count():
            raise ValueError(f"userId {args.user_id} does not exist in the saved model scope")
    elif args.all_users:
        users = model.userFactors.selectExpr("cast(id as int) userId")
    else:
        users = spark.read.parquet(args.users_file).selectExpr("cast(userId as int) userId").distinct()
        unknown_users = users.join(model.userFactors.selectExpr("id userId"), "userId", "left_anti")
        if unknown_users.limit(1).count():
            raise ValueError("users-file contains users outside the saved model scope")
    output = recommend_unseen(
        model, train, movies, users, args.top_n, RECOMMENDATION_CANDIDATE_MULTIPLIER
    )
    output.write.mode("errorifexists").parquet(args.output)
    rows = spark.read.parquet(args.output)
    print({"output": args.output, "rows": rows.count(),
           "users": rows.select("userId").distinct().count()}, flush=True)
    spark.stop()


if __name__ == "__main__":
    main()
