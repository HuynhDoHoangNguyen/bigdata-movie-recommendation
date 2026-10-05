"""Bounded read-only ranking diagnosis using the saved SAMPLE model; never fit."""

import hashlib
import json
import math
import time
from datetime import datetime, timezone

from pyspark import StorageLevel
from pyspark.ml.recommendation import ALSModel
from pyspark.sql import SparkSession, Window, functions as F

from config import (EXPERIMENTS_PATH, FINAL_METRICS_PATH, MANIFEST_PATH, MODEL_PATH,
                    MOVIES_PATH, RECOMMENDATIONS_PATH, SAMPLE_USERS_PATH, SEED,
                    TEST_PATH, TOPK_METRICS_PATH, TRAIN_PATH, TUNING_MANIFEST_PATH)
from output_contract import read_manifest, require_new_outputs, write_manifest
from topn import deterministic_user_sample, evaluate_top_k


OUTPUT = "hdfs://namenode:9000/project/movielens/output/tv3/diagnostics/ranking_phase_c"
KS = (10, 20, 50, 100)


def fingerprint(spark):
    """Fingerprint every protected TV3 artifact with HDFS file checksums."""
    entries = []
    for base in (MODEL_PATH, FINAL_METRICS_PATH, TOPK_METRICS_PATH, RECOMMENDATIONS_PATH,
                 EXPERIMENTS_PATH, MANIFEST_PATH, SAMPLE_USERS_PATH, TUNING_MANIFEST_PATH):
        hpath = spark._jvm.org.apache.hadoop.fs.Path(base)
        fs = hpath.getFileSystem(spark._jsc.hadoopConfiguration())
        files = fs.listFiles(hpath, True)
        while files.hasNext():
            item = files.next()
            entries.append((item.getPath().toString(), item.getLen(), item.getModificationTime(),
                            str(fs.getFileChecksum(item.getPath()))))
    entries.sort()
    return {"files": len(entries), "sha256": hashlib.sha256(json.dumps(entries).encode()).hexdigest()}


def join_metrics(recs, relevant, users, k):
    """Independent pair-join hits, including users with zero recommendations."""
    totals = relevant.groupBy("userId").count().withColumnRenamed("count", "relevant_count")
    hits = recs.select("userId", "movieId").join(relevant, ["userId", "movieId"], "inner").groupBy(
        "userId"
    ).count().withColumnRenamed("count", "hits")
    counts = recs.groupBy("userId").count().withColumnRenamed("count", "recommendation_count")
    per_user = users.join(totals, "userId").join(hits, "userId", "left").join(counts, "userId", "left").fillna(
        0, ["hits", "recommendation_count"]
    )
    stats = per_user.agg(
        F.count("*").alias("users"), F.sum("hits").alias("hits"),
        F.avg(F.col("hits") / float(k)).alias("precision"),
        F.avg(F.col("hits") / F.col("relevant_count")).alias("recall"),
        F.avg((F.col("hits") > 0).cast("double")).alias("hit_rate"),
        F.sum("recommendation_count").alias("recommendations"),
        F.min("recommendation_count").alias("min_recommendations_per_user"),
        F.max("recommendation_count").alias("max_recommendations_per_user"),
        F.avg("recommendation_count").alias("avg_recommendations_per_user"),
        F.sum((F.col("recommendation_count") < k).cast("long")).alias("users_with_fewer_than_k"),
    ).first().asDict()
    return {"k": k, **stats}


def synthetic_metric_check(spark):
    """Known nonzero fixture exercises the existing helper without fitting ALS."""
    train = spark.createDataFrame([(1, 1, 5.0), (2, 1, 5.0)], "userId int,movieId int,rating double")
    test = spark.createDataFrame([(1, 2, 5.0), (1, 3, 4.0), (2, 4, 4.0)],
                                 "userId int,movieId int,rating double")
    movies = spark.createDataFrame([(n, str(n), 'Drama') for n in range(1, 6)],
                                   "movieId int,title string,genres string")

    class FixtureModel:
        def recommendForUserSubset(self, users, n):
            return spark.createDataFrame([
                (1, [(1, 6.0), (2, 5.0), (5, 4.5)]),
                (2, [(1, 6.0), (4, 5.0), (5, 4.5)]),
            ], "userId int,recommendations array<struct<movieId:int,rating:float>>")

    result = evaluate_top_k(FixtureModel(), train, test, movies, 10, 4.0, 2, 10, SEED)
    if not (math.isclose(result["precision_at_k"], 0.1) and math.isclose(result["recall_at_k"], 0.75)
            and result["total_hits"] == 2 and result["recommended_item_count"] == 4):
        raise ValueError(f"Existing metric failed nonzero fixture: {result}")
    return {"status": "PASS", "precision_at_10": result["precision_at_k"],
            "recall_at_10": result["recall_at_k"], "hits": result["total_hits"]}


def main():
    started = time.perf_counter()
    spark = SparkSession.builder.appName("TV3RankingDiagnostic").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    cached = []

    def cache(frame):
        frame = frame.persist(StorageLevel.DISK_ONLY)
        cached.append(frame)
        return frame

    try:
        require_new_outputs(spark, [OUTPUT])
        before = fingerprint(spark)
        manifest = read_manifest(spark, MANIFEST_PATH)
        if manifest["status"] != "SAMPLE_COMPLETE_WARNING" or manifest["model_scope"] != "SAMPLE":
            raise ValueError("Requires saved Phase B SAMPLE model")
        fixture = synthetic_metric_check(spark)
        model = ALSModel.load(MODEL_PATH)
        sample_users = spark.read.parquet(SAMPLE_USERS_PATH)
        train = cache(spark.read.parquet(TRAIN_PATH).join(F.broadcast(sample_users), "userId", "inner"))
        test = cache(spark.read.parquet(TEST_PATH).join(F.broadcast(sample_users), "userId", "inner"))
        movies = spark.read.parquet(MOVIES_PATH)
        all_relevant = cache(test.where("rating >= 4.0").select("userId", "movieId").distinct())
        users = cache(deterministic_user_sample(all_relevant, 500, SEED))
        relevant = cache(all_relevant.join(F.broadcast(users), "userId", "inner"))
        user_factors = model.userFactors.select(F.col("id").alias("userId"))
        item_factors = model.itemFactors.select(F.col("id").alias("movieId"))
        if users.count() != 500 or users.join(user_factors, "userId", "left_anti").count():
            raise ValueError("Evaluation users do not match the model scope")
        known_relevant = cache(relevant.join(item_factors, "movieId", "inner"))
        print("[1/4] Generating one bounded ALS pool: 500 users x up to 1000 candidates", flush=True)
        raw = cache(model.recommendForUserSubset(users, 1000).select(
            "userId", F.posexplode("recommendations").alias("pos", "rec")
        ).select("userId", (F.col("pos") + 1).alias("raw_rank"),
                 F.col("rec.movieId").cast("int").alias("movieId"),
                 F.col("rec.rating").cast("double").alias("prediction")))
        watched = cache(train.join(F.broadcast(users), "userId", "inner").select("userId", "movieId").distinct())
        unseen = cache(raw.join(watched, ["userId", "movieId"], "left_anti"))
        order = Window.partitionBy("userId").orderBy(F.desc("prediction"), F.asc("movieId"))
        ranking, coverage, ranked_frames = [], [], {}
        for k in KS:
            candidate_n = k * 10
            pool = unseen.where(F.col("raw_rank") <= candidate_n)
            ranked = cache(pool.withColumn("rank", F.row_number().over(order)).where(F.col("rank") <= k).join(
                F.broadcast(movies), "movieId", "inner"
            ))
            ranked_frames[k] = ranked
            ranking.append(join_metrics(ranked, relevant, users, k))
            raw_pool = raw.where(F.col("raw_rank") <= candidate_n)
            coverage.append({"k": k, "candidate_n": candidate_n,
                "raw_candidates": raw_pool.count(), "after_history_filter": pool.count(),
                "avg_raw_candidates_per_user": raw_pool.count() / 500.0,
                "avg_unseen_candidates_per_user": pool.count() / 500.0,
                "relevant_pairs_in_raw_pool": relevant.join(raw_pool, ["userId", "movieId"]).count(),
                "relevant_pairs_after_history_filter": relevant.join(pool, ["userId", "movieId"]).count(),
                "missing_metadata_candidates": pool.join(movies, "movieId", "left_anti").count()})
        original = spark.read.parquet(TOPK_METRICS_PATH).first().asDict()
        if not (math.isclose(ranking[0]["precision"], original["precision_at_k"])
                and math.isclose(ranking[0]["recall"], original["recall_at_k"])):
            raise ValueError("Independent Top-10 differs from the protected Phase B metric")
        print("[2/4] Auditing eligibility, score distributions and exact ID joins", flush=True)
        sample_relevant_movies = all_relevant.select("movieId").distinct()
        eligibility = {"model_item_factors": item_factors.count(),
            "sample_relevant_movies": sample_relevant_movies.count(),
            "sample_relevant_movies_with_factors": sample_relevant_movies.join(item_factors, "movieId").count(),
            "evaluation_relevant_pairs": relevant.count(), "evaluation_relevant_movies": relevant.select("movieId").distinct().count(),
            "evaluation_relevant_pairs_with_factors": known_relevant.count(),
            "evaluation_relevant_pairs_without_factors": relevant.join(item_factors, "movieId", "left_anti").count(),
            "eligible_relevant_pairs_outside_top1000_pool": known_relevant.join(raw, ["userId", "movieId"], "left_anti").count(),
            "relevant_history_overlap": relevant.join(watched, ["userId", "movieId"]).count(),
            "sample_train_test_overlap": train.join(test, ["userId", "movieId"]).count(),
            "top10_duplicate_pairs": ranked_frames[10].groupBy("userId", "movieId").count().where("count > 1").count(),
            "top10_history_overlap": ranked_frames[10].join(watched, ["userId", "movieId"]).count(),
            "movie_id_types": {"raw": raw.schema["movieId"].dataType.simpleString(),
                               "relevant": relevant.schema["movieId"].dataType.simpleString()}}
        popularity = cache(train.groupBy("movieId").agg(F.count("*").alias("train_count"), F.avg("rating").alias("train_mean")))
        score_summary = ranked_frames[10].join(popularity, "movieId").agg(
            F.min("prediction").alias("min_score"), F.max("prediction").alias("max_score"),
            F.avg("prediction").alias("mean_score"),
            F.avg("train_count").alias("mean_item_train_count"),
            F.sum((F.col("train_count") <= 5).cast("long")).alias("recommendations_from_items_with_at_most_5_train_rows"),
            F.countDistinct("movieId").alias("distinct_top10_movies"),
        ).first().asDict()
        # Do not feed a frame joined to ALS itemFactors back into transform:
        # Spark detects ambiguous self-join lineage inside the model's factor joins.
        relevant_scores = model.transform(test.where("rating >= 4.0").join(F.broadcast(users), "userId"))
        score_summary["relevant_prediction_stats"] = relevant_scores.agg(
            F.min("prediction").alias("min"), F.max("prediction").alias("max"), F.avg("prediction").alias("mean")
        ).first().asDict()
        # Rebuild the bounded 5,000-row frame to detach factor-join lineage;
        # records stay distributed, never collected to the driver.
        score_input = spark.createDataFrame(ranked_frames[10].select("userId", "movieId", "prediction").rdd,
                                            "userId int,movieId int,recommendation_score double")
        score_summary["recommendation_score_consistency"] = model.transform(score_input).agg(
            F.max(F.abs(F.col("prediction") - F.col("recommendation_score"))).alias("max_absolute_difference"),
            F.sum((F.abs(F.col("prediction") - F.col("recommendation_score")) > 0.0001).cast("long"))
            .alias("mismatches_above_1e_4"),
        ).first().asDict()
        print("[3/4] Train-only popularity baseline on the same 500 users", flush=True)
        # Bounded 500 x 1000 comparison grid, never full model users x catalog.
        popular = popularity.join(item_factors, "movieId").join(movies.select("movieId"), "movieId").orderBy(
            F.desc("train_count"), F.asc("movieId")
        ).limit(1000)
        popular = popular.withColumn("pop_rank", F.row_number().over(Window.orderBy(F.desc("train_count"), "movieId")))
        baseline_pool = cache(users.crossJoin(F.broadcast(popular)).join(watched, ["userId", "movieId"], "left_anti"))
        baseline = []
        for k in KS:
            candidates = baseline_pool.where(F.col("pop_rank") <= k * 10)
            recs = candidates.withColumn("rank", F.row_number().over(
                Window.partitionBy("userId").orderBy(F.desc("train_count"), "movieId")
            )).where(F.col("rank") <= k)
            baseline.append(join_metrics(recs, relevant, users, k))
        print("[4/4] Five deterministic case studies; preserving existing artifacts", flush=True)
        case_users = [r.userId for r in deterministic_user_sample(users, 5, SEED).orderBy("userId").collect()]
        cases = []
        for uid in case_users:
            rel = [r.movieId for r in relevant.where(F.col("userId") == uid).orderBy("movieId").collect()]
            recs = [r.asDict() for r in ranked_frames[10].where(F.col("userId") == uid).orderBy("rank").select(
                "movieId", "prediction", "rank"
            ).collect()]
            case = {"userId": uid, "train_history_count": watched.where(F.col("userId") == uid).count(),
                "relevant_test_count": len(rel), "relevant_movie_ids": rel, "top10": recs,
                "intersection_count": len(set(rel).intersection(r["movieId"] for r in recs)),
                "relevant_items_with_factors": known_relevant.where(F.col("userId") == uid).count()}
            case["relevant_items_in_raw_top100"] = relevant.where(F.col("userId") == uid).join(
                raw.where(F.col("raw_rank") <= 100), ["userId", "movieId"]).count()
            case["relevant_items_in_raw_top1000"] = relevant.where(F.col("userId") == uid).join(
                raw, ["userId", "movieId"]).count()
            cases.append(case)
        after = fingerprint(spark)
        if before != after:
            raise ValueError("Protected artifact fingerprints changed")
        result = {"status": "DIAGNOSTIC_COMPLETE", "scope": "SAMPLE_500_ELIGIBLE_USERS",
            "generated_at_utc": datetime.now(timezone.utc).isoformat(), "seed": SEED,
            "original_top10": {"precision": original["precision_at_k"], "recall": original["recall_at_k"]},
            "synthetic_metric_check": fixture, "als_ranking": ranking, "candidate_coverage": coverage,
            "eligibility": eligibility, "top10_score_audit": score_summary,
            "popularity_baseline": {"status": "PASS", "fit_source": "SAMPLE TV2 TRAIN ONLY",
                "candidate_item_eligibility": "same model item factors and STANDARD metadata as ALS",
                "candidate_buffer": "10*K; popularity-ordered rather than ALS-score-ordered", "ranking": baseline},
            "case_studies": cases, "protected_artifacts": {"before": before, "after": after, "unchanged": True},
            "runtime_sec": time.perf_counter() - started}
        write_manifest(spark, result, OUTPUT)
        print(json.dumps({key: value for key, value in result.items() if key != "case_studies"}, indent=2), flush=True)
        print(f"Five bounded case studies stored in {OUTPUT}", flush=True)
    finally:
        for frame in reversed(cached):
            frame.unpersist()
        spark.stop()


if __name__ == "__main__":
    main()
