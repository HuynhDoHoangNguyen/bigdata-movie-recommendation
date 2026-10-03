"""Central configuration for the TV3 ALS pipeline."""

TV2_BASE = "hdfs://namenode:9000/project/movielens/output/tv2"
TRAIN_PATH = f"{TV2_BASE}/als/train"
TEST_PATH = f"{TV2_BASE}/als/test"
MOVIES_PATH = "hdfs://namenode:9000/project/movielens/standard/movies"

OUTPUT_BASE = "hdfs://namenode:9000/project/movielens/output/tv3"
MODEL_PATH = f"{OUTPUT_BASE}/model/als_best"
EXPERIMENTS_PATH = f"{OUTPUT_BASE}/metrics/experiments"
FINAL_METRICS_PATH = f"{OUTPUT_BASE}/metrics/final"
TOPK_METRICS_PATH = f"{OUTPUT_BASE}/metrics/top_k"
RECOMMENDATIONS_PATH = f"{OUTPUT_BASE}/recommendations/topn"
MANIFEST_PATH = f"{OUTPUT_BASE}/manifest"

EXPECTED_ALS_COLUMNS = ["userId", "movieId", "rating"]
EXPECTED_MOVIE_COLUMNS = ["movieId", "title", "genres"]
EXPECTED_TRAIN_COUNT = 25_520_897
EXPECTED_TEST_COUNT = 6_479_307

SEED = 42
VALIDATION_RATIO = 0.1
TUNING_MAX_USERS = 10_000
FINAL_MAX_USERS = 10_000
TOP_K = 10
RELEVANCE_THRESHOLD = 4.0
TOP_K_MAX_USERS = 5_000
TOP_N = 10
TOP_N_SAMPLE_USERS = 1_000
RECOMMENDATION_CANDIDATE_MULTIPLIER = 10

# Three controlled experiments suitable for the small local Spark cluster.
EXPERIMENTS = [
    {"experiment_id": "als_a", "rank": 10, "maxIter": 5, "regParam": 0.10},
    {"experiment_id": "als_b", "rank": 10, "maxIter": 8, "regParam": 0.10},
    {"experiment_id": "als_c", "rank": 10, "maxIter": 8, "regParam": 0.05},
]

# Selected only from the deterministic tuning sample; not a full-data claim.
BEST_SAMPLE_PARAMS = {"rank": 10, "maxIter": 8, "regParam": 0.05}
