"""Configuration shared by the TV2 processing pipeline."""

STANDARD_BASE = "hdfs://namenode:9000/project/movielens/standard"
OUTPUT_BASE = "hdfs://namenode:9000/project/movielens/output/tv2"
STANDARD_PATHS = {name: f"{STANDARD_BASE}/{name}" for name in ("ratings", "movies", "tags", "links")}
PROCESSED_PATHS = {
    "ratings": f"{OUTPUT_BASE}/processed/ratings",
    "ratings_movies": f"{OUTPUT_BASE}/processed/ratings_movies",
}
ALS_PATHS = {name: f"{OUTPUT_BASE}/als/{name}" for name in ("train", "test")}
ANALYTICS_PATHS = {name: f"{OUTPUT_BASE}/analytics/{name}" for name in (
    "dataset_summary", "rating_distribution", "user_activity", "movie_popularity",
    "genre_statistics", "rating_trend", "tag_statistics")}
MANIFEST_PATH = f"{OUTPUT_BASE}/manifest"
SEED = 42
TRAIN_RATIO = 0.8
HIGHLY_RATED_MIN_COUNT = 1000
EXPECTED_COUNTS = {"ratings": 32_000_204, "movies": 87_585, "tags": 2_000_072, "links": 87_585}
