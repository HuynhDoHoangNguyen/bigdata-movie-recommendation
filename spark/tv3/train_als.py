"""ALS estimator construction and timed model fitting."""

import time

from pyspark.ml.recommendation import ALS


def build_estimator(params, seed):
    return ALS(
        userCol="userId",
        itemCol="movieId",
        ratingCol="rating",
        implicitPrefs=False,
        coldStartStrategy="drop",
        nonnegative=False,
        rank=int(params["rank"]),
        maxIter=int(params["maxIter"]),
        regParam=float(params["regParam"]),
        seed=int(seed),
    )


def fit_als(train, params, seed):
    started = time.perf_counter()
    model = build_estimator(params, seed).fit(train)
    return model, time.perf_counter() - started
