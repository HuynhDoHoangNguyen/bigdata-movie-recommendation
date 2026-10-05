"""Support-aware ranking only; no fitting or writes to existing ALS artifacts."""

import math

K = 10
POOL_SIZE = 1000
METHODS = (
    {"method": "RAW_ALS", "parameter": 0.0},
    *({"method": "HARD_GATE", "parameter": float(n)} for n in (2, 5, 10, 20)),
    *({"method": "SHRINKAGE", "parameter": float(n)} for n in (5, 10, 20, 50)),
    *({"method": "HYBRID", "parameter": n} for n in (0.25, 0.50, 0.75)),
    {"method": "POPULARITY", "parameter": 0.0},
)


def shrink_score(prediction, support, alpha, global_mean):
    """Reference scalar formula used in regression checks, without clipping."""
    if alpha <= 0 or support < 0:
        raise ValueError("Requires positive alpha and nonnegative support")
    return global_mean + support / (support + alpha) * (prediction - global_mean)


def selection_key(row):
    """Popularity is a reference only and must not enter reranking selection."""
    complexity = {"RAW_ALS": 0, "HARD_GATE": 1, "SHRINKAGE": 2, "HYBRID": 3}
    if row["method"] not in complexity:
        raise ValueError("Popularity baseline is not a selectable ALS reranker")
    return (-row["recall"], -row["precision"], -row["hit_rate"],
            -row["users_with_10"], -row["recommendations"],
            complexity[row["method"]], row["parameter"])


def choose_config(results):
    eligible = [row for row in results if row["method"] != "POPULARITY"]
    if not eligible:
        raise ValueError("No selectable results")
    return min(eligible, key=selection_key)


def item_support(train_core):
    from pyspark.sql import functions as F
    return train_core.groupBy("movieId").agg(F.count("*").alias("train_support"))


def rank_candidates(pool, method, parameter, global_mean):
    """Pool already has train-core support, metadata and history exclusion."""
    from pyspark.sql import Window, functions as F
    if not any(method == config["method"] and float(parameter) == config["parameter"]
               for config in METHODS):
        raise ValueError(f"Unsupported locked config: {method}/{parameter}")
    window = Window.partitionBy("userId")
    if method == "RAW_ALS":
        frame = pool.where(F.col("raw_rank") <= 100).withColumn("adjusted_score", F.col("prediction"))
    elif method == "HARD_GATE":
        frame = pool.where(F.col("train_support") >= int(parameter)).withColumn(
            "adjusted_score", F.col("prediction"))
    elif method == "SHRINKAGE":
        reliability = F.col("train_support") / (F.col("train_support") + float(parameter))
        frame = pool.withColumn("adjusted_score", F.lit(float(global_mean)) + reliability * (
            F.col("prediction") - float(global_mean)))
    elif method == "HYBRID":
        frame = pool.withColumn("_als_rank", F.row_number().over(window.orderBy(
            F.desc("prediction"), F.asc("movieId")))).withColumn(
            "_pop_rank", F.row_number().over(window.orderBy(F.desc("train_support"), F.asc("movieId"))))
        frame = frame.withColumn("_max_rank", F.count("*").over(window)).withColumn(
            "adjusted_score", float(parameter) * (1 - (F.col("_als_rank") - 1) / F.col("_max_rank"))
            + (1 - float(parameter)) * (1 - (F.col("_pop_rank") - 1) / F.col("_max_rank")))
    else:
        frame = pool.withColumn("adjusted_score", F.col("train_support").cast("double"))
    return frame.withColumn("rank", F.row_number().over(window.orderBy(
        F.desc("adjusted_score"), F.asc("movieId")))).where(F.col("rank") <= K).select(
        "userId", "movieId", "title", "genres", F.col("prediction").cast("double"),
        F.col("adjusted_score").cast("double"), F.col("train_support").cast("long"), "rank")


def verify_order(frame):
    from pyspark.sql import Window, functions as F
    order = Window.partitionBy("userId").orderBy(F.desc("adjusted_score"), F.asc("movieId"))
    mismatches = frame.withColumn("_expected_rank", F.row_number().over(order)).where(
        F.col("rank") != F.col("_expected_rank")).count()
    if mismatches:
        raise ValueError(f"Non-deterministic or non-contiguous rank: {mismatches}")


def verify_recommendations(frame, history, movies, users, require_full=False):
    from pyspark.sql import functions as F
    expected = [("userId", "int"), ("movieId", "int"), ("title", "string"), ("genres", "string"),
                ("prediction", "double"), ("adjusted_score", "double"), ("train_support", "bigint"), ("rank", "int")]
    if [(field.name, field.dataType.simpleString()) for field in frame.schema] != expected:
        raise ValueError(f"Reranked schema mismatch: {frame.schema.simpleString()}")
    invalid = F.lit(False)
    for name, _ in expected:
        invalid = invalid | F.col(name).isNull()
    for name in ("prediction", "adjusted_score"):
        invalid = invalid | F.isnan(name) | (F.abs(F.col(name)) == float("inf"))
    invalid = invalid | (F.col("rank") < 1) | (F.col("rank") > K) | (F.col("train_support") < 0)
    audit = {"rows": frame.count(), "users": frame.select("userId").distinct().count(),
             "invalid_rows": frame.where(invalid).count(),
             "duplicate_pairs": frame.groupBy("userId", "movieId").count().where("count > 1").count(),
             "duplicate_ranks": frame.groupBy("userId", "rank").count().where("count > 1").count(),
             "history_overlap": frame.select("userId", "movieId").join(history, ["userId", "movieId"]).count(),
             "unexpected_users": frame.join(users, "userId", "left_anti").count()}
    meta = frame.alias("r").join(movies.alias("m"), "movieId", "left")
    audit["metadata_mismatches"] = meta.where(F.col("m.movieId").isNull()
        | ~F.col("r.title").eqNullSafe(F.col("m.title"))
        | ~F.col("r.genres").eqNullSafe(F.col("m.genres"))).count()
    if any(value for key, value in audit.items() if key not in ("rows", "users")):
        raise ValueError(f"Reranking contract failed: {audit}")
    verify_order(frame)
    if require_full and (audit["users"] != users.count() or audit["rows"] != users.count() * K):
        raise ValueError(f"Expected {K} recommendations per demo user: {audit}")
    return audit


def ranking_metrics(frame, relevant, users):
    """Verify independent pair-join metrics against array intersection metrics."""
    from pyspark.sql import functions as F
    from ranking_diagnostic import join_metrics
    pair = join_metrics(frame, relevant, users, K)
    rel = relevant.groupBy("userId").agg(F.collect_set("movieId").alias("_rel"))
    rec = frame.groupBy("userId").agg(F.collect_set("movieId").alias("_rec"))
    per_user = users.join(rel, "userId").join(rec, "userId", "left").withColumn(
        "_rec", F.coalesce("_rec", F.array().cast("array<int>"))).withColumn(
        "_hits", F.size(F.array_intersect("_rel", "_rec")))
    independent = per_user.agg(F.avg(F.col("_hits") / float(K)).alias("precision"),
        F.avg(F.col("_hits") / F.size("_rel")).alias("recall"),
        F.avg((F.col("_hits") > 0).cast("double")).alias("hit_rate"),
        F.sum("_hits").alias("hits"),
        F.sum((F.size("_rec") == K).cast("long")).alias("users_with_10")).first().asDict()
    for name in ("precision", "recall", "hit_rate", "hits"):
        if not math.isclose(pair[name], independent[name], rel_tol=1e-9, abs_tol=1e-12):
            raise ValueError(f"Independent ranking metrics differ: {name}")
    pair.update({"users_with_10": independent["users_with_10"],
        "mean_item_support": frame.agg(F.avg("train_support")).first()[0] or 0.0,
        "metric_verification": "PASS"})
    return pair


def regression_checks(spark):
    """Known expected behavior, including empty users, leakage and score ties."""
    from pyspark.sql import functions as F
    from evaluate import split_train_validation
    if not math.isclose(shrink_score(8.0, 1, 10, 3.5), 3.5 + 4.5 / 11):
        raise AssertionError("Shrinkage formula")
    if shrink_score(8.0, 0, 10, 3.5) != 3.5 or shrink_score(8.0, 1000, 10, 3.5) <= 5:
        raise AssertionError("Zero support or unwanted score clipping")
    core = spark.createDataFrame([(1, 1, 3.0), (2, 1, 4.0), (3, 2, 5.0)],
                                 "userId int,movieId int,rating double")
    labels = spark.createDataFrame([(1, 2), (1, 3), (2, 4)], "userId int,movieId int")
    support = item_support(core)
    if {r.movieId: r.train_support for r in support.collect()} != {1: 2, 2: 1}:
        raise AssertionError("Support must count train-core only")
    # Label-only items must stay zero support; labels are not passed to item_support.
    if labels.join(support, "movieId", "left").fillna(0, ["train_support"]).where(
            "movieId IN (3, 4) AND train_support != 0").count():
        raise AssertionError("Label-only item support leakage")
    movies = spark.createDataFrame([(n, str(n), "Drama") for n in range(1, 6)],
                                   "movieId int,title string,genres string")
    users = spark.createDataFrame([(1,), (2,)], "userId int")
    pool = spark.createDataFrame([(1, 2, 8.0, 1, 1), (1, 3, 5.0, 2, 100),
                                  (1, 4, 5.0, 3, 100), (2, 5, 4.0, 1, 0)],
                                 "userId int,movieId int,prediction double,raw_rank int,train_support long").join(movies, "movieId")
    shrunk = rank_candidates(pool, "SHRINKAGE", 10, 3.5)
    if [r.movieId for r in shrunk.where("userId = 1").orderBy("rank").select("movieId").collect()] != [3, 4, 2]:
        raise AssertionError("Shrinkage must demote inflated low-support score and break ties by movieId")
    gated = rank_candidates(pool, "HARD_GATE", 5, 3.5)
    metrics = ranking_metrics(gated, labels, users)
    if not (metrics["users"] == 2 and metrics["hits"] == 1 and metrics["users_with_fewer_than_k"] == 2
            and math.isclose(metrics["precision"], 0.05) and math.isclose(metrics["recall"], 0.25)
            and math.isclose(metrics["hit_rate"], 0.5)):
        raise AssertionError(f"Sparse users / metric denominators: {metrics}")
    history = spark.createDataFrame([(1, 2)], "userId int,movieId int")
    unseen = pool.join(history, ["userId", "movieId"], "left_anti")
    verify_recommendations(rank_candidates(unseen, "SHRINKAGE", 10, 3.5), history, movies, users)
    split_input = spark.createDataFrame([(1, n, float(n)) for n in range(1, 6)] + [(2, 1, 3.0)],
                                        "userId int,movieId int,rating double")
    train_core, validation = split_train_validation(split_input, 0.1, 42)
    if train_core.count() != 5 or validation.count() != 1 or train_core.join(
            validation, ["userId", "movieId"]).count():
        raise AssertionError("Original deterministic split / leakage")
    if train_core.exceptAll(split_train_validation(split_input.repartition(2), 0.1, 42)[0]).count():
        raise AssertionError("Split must be deterministic after repartitioning")
    template = {"precision": 0.1, "recall": 0.1, "hit_rate": 0.1, "users_with_10": 500,
                "recommendations": 5000}
    if choose_config([{**template, "method": "SHRINKAGE", "parameter": 10.0},
                      {**template, "method": "HARD_GATE", "parameter": 5.0},
                      {**template, "method": "POPULARITY", "parameter": 0.0}])["method"] != "HARD_GATE":
        raise AssertionError("Simple config tie-break / popularity excluded")
    return {"status": "PASS", "shrinkage": "PASS", "support_source": "PASS",
            "split_and_label_leakage": "PASS", "deterministic_order": "PASS",
            "history_exclusion": "PASS", "independent_metrics": "PASS", "selection": "PASS"}
