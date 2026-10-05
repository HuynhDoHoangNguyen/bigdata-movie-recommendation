"""Scalable unseen-item Top-N recommendation and Top-K evaluation."""

from pyspark.sql import Window, functions as F


def deterministic_user_sample(users, limit, seed):
    if limit is None:
        return users.select("userId").distinct()
    return (
        users.select("userId").distinct()
        .orderBy(F.xxhash64("userId", F.lit(seed)), "userId")
        .limit(int(limit))
    )


def recommend_unseen(
    model,
    train,
    movies,
    users,
    top_n,
    candidate_multiplier,
):
    """Generate Top-N, remove training history, and attach movie metadata."""
    candidate_n = max(int(top_n), int(top_n) * int(candidate_multiplier))
    requested_users = users.select(F.col("userId").cast("int")).distinct()
    raw = model.recommendForUserSubset(requested_users, candidate_n)
    exploded = raw.select(
        "userId",
        F.explode("recommendations").alias("recommendation"),
    ).select(
        "userId",
        F.col("recommendation.movieId").cast("int").alias("movieId"),
        F.col("recommendation.rating").cast("double").alias("prediction"),
    )
    watched = train.select("userId", "movieId").distinct()
    unseen = exploded.join(watched, ["userId", "movieId"], "left_anti")
    rank_window = Window.partitionBy("userId").orderBy(
        F.desc("prediction"), F.asc("movieId")
    )
    ranked = (
        unseen.withColumn("rank", F.row_number().over(rank_window))
        .where(F.col("rank") <= int(top_n))
    )
    return (
        ranked.join(F.broadcast(movies), "movieId", "inner")
        .select("userId", "movieId", "title", "genres", "prediction", "rank")
    )


def evaluate_top_k(
    model,
    train,
    test,
    movies,
    k,
    relevance_threshold,
    max_users,
    candidate_multiplier,
    seed,
):
    """Evaluate recommendations for users with at least one relevant test item."""
    relevant = (
        test.where(F.col("rating") >= float(relevance_threshold))
        .groupBy("userId")
        .agg(F.collect_set("movieId").alias("relevant_items"))
    )
    eligible_users = relevant.count()
    selected_users = deterministic_user_sample(relevant, max_users, seed)
    selected_count = selected_users.count()
    recommendations = recommend_unseen(
        model,
        train,
        movies,
        selected_users,
        k,
        candidate_multiplier,
    )
    recommended = recommendations.groupBy("userId").agg(
        F.collect_list("movieId").alias("recommended_items")
    )
    per_user = (
        selected_users.join(relevant, "userId", "inner")
        .join(recommended, "userId", "left")
        .withColumn(
            "recommended_items",
            F.coalesce("recommended_items", F.array().cast("array<int>")),
        )
        .withColumn(
            "hits", F.size(F.array_intersect("recommended_items", "relevant_items"))
        )
        .withColumn("precision_at_k", F.col("hits") / F.lit(float(k)))
        .withColumn("recall_at_k", F.col("hits") / F.size("relevant_items"))
    )
    aggregate = per_user.agg(
        F.count("*").alias("evaluated_users"),
        F.avg("precision_at_k").alias("precision_at_k"),
        F.avg("recall_at_k").alias("recall_at_k"),
        F.sum("hits").alias("total_hits"),
        F.sum((F.col("hits") > 0).cast("long")).alias("users_with_hits"),
        F.sum(F.size("recommended_items")).alias("recommended_item_count"),
        F.sum(F.size("relevant_items")).alias("relevant_item_count"),
    ).first().asDict()
    aggregate.update({
        "k": int(k),
        "relevance_threshold": float(relevance_threshold),
        "relevance_definition": f"test rating >= {float(relevance_threshold):.1f}",
        "eligible_users_full": int(eligible_users),
        "selected_users": int(selected_count),
        "scope": "FULL" if max_users is None else "DETERMINISTIC_SAMPLE",
        "status": "PASS",
    })
    return aggregate
