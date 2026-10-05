# TV3 PHASE D RUNTIME COMPLETION REPORT

Ngày hoàn tất: 05/10/2026 (Asia/Saigon).
**PHASE_D_COMPLETE — READY TO CLOSE TV3 SAMPLE: YES, với giới hạn chất lượng đã nêu.**

Đã fit đúng một temporary selection model trên train_core, chọn và khóa
**SHRINKAGE alpha=50** bằng validation chưa được model tạm thấy. Sau đó áp dụng
config này đúng một lần lên saved Phase B SAMPLE model để test confirmation.
Reranked test có **451 hits** so với raw ALS **0**; popularity vẫn cao hơn với
**627 hits**. Đã xuất riêng 200 reranked recommendations cho 20 demo users,
verifier PASS, HDFS HEALTHY. Model, metrics và Top-N Phase B/C giữ nguyên.

Evidence đầy đủ, không làm tròn:
[TV3_RERANKING_PHASE_D_RESULTS.json](TV3_RERANKING_PHASE_D_RESULTS.json).
Không chạy Full ALS, không tuning lại ALS A/B/C, không commit/push/merge.

## 1. Temporary selection model

**PASS**. Model này chỉ dùng để selection, không phải final model cho TV4.

| Hạng mục | Kết quả |
|---|---:|
| Saved deterministic sample users | 10.000 |
| Sample train rows | 1.243.712 |
| Temporary model fit rows, train_core ONLY | 1.123.778 |
| Validation rows, không nằm trong fit | 119.934 |
| Core-validation pair overlap | 0 |
| ALS fit calls trong Phase D | **1** |
| rank / maxIter / regParam / seed | 10 / 8 / 0.05 / 42 |
| implicitPrefs / coldStartStrategy | False / drop |
| Reloaded user / item factors | 10.000 / 26.905 |
| Invalid factor rows | 0 |
| Fit runtime | 7.200765 giây |
| Fit stage runtime trong script | 49.539346 giây |

Temporary path:
`hdfs://namenode:9000/project/movielens/output/tv3/diagnostics/reranking_phase_d/selection_model`.

Original sample/split được tái tạo bằng helper hiện có, không đổi seed/hash/split.
Đối chiếu exact sample user IDs, train counts, core-validation union và overlap;
factors reload khớp exact user/item IDs của train_core. Fit attempt được lưu
trước khi fit; stage từ chối model/attempt đã tồn tại để ngăn fit lần thứ hai.
Regression checks đã PASS trước workload thực.

Saved Phase B model vẫn ở `model/als_best`, fit trên toàn bộ sample train.
Nó được reload cho confirmation/demo, không dùng cho Phase D selection.
Validation độc lập với **training của selection model**; đây vẫn là validation
Phase A từng dùng để chọn fixed ALS params, không phải một holdout mới của cả
quá trình phát triển.

## 2. Support distribution and raw validation

Support = số ratings/movie từ **train_core ONLY**, dùng nhất quán ở validation,
confirmation và demo. Không sử dụng validation/test counts làm support feature.

| Train-core support statistic | Giá trị |
|---|---:|
| Supported items | 26.905 |
| Min | 1 |
| Median | 3 |
| P75 | 14 |
| P90 | 79 |
| P95 | 197 |
| Max | 3.761 |
| Tổng support | 1.123.778 |
| Global train-core mean rating | 3.5436634281859942 |
| RAW ALS validation Top-10 mean core support | 4.3100 |

Percentiles dùng `percentile_approx`, accuracy 10.000, trên supported core items;
model items không có core support được gán 0 khi join feature ở confirmation.
Raw validation **P@10=0, R@10=0, HR@10=0**, 5.000 recs, 500 users đủ 10.

## 3. Validation experiments: hard gate, shrinkage, hybrid and popularity

Tất cả dùng cùng 500 deterministic eligible validation users, relevance
`validation.rating >= 4.0`, exclude train_core history. User sampling giữ nguyên
`xxhash64(userId, lit(42))`, tie-break userId. Không đọc TV2 TEST trong selection.

| Method | Parameter | P@10 | R@10 | HR@10 | Users | Users with 10 | Mean core support | Hits |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| RAW_ALS | 0.0 | 0.000000 | 0.000000 | 0.000000 | 500 | 500 | 4.3100 | 0 |
| HARD_GATE | 2.0 | 0.000000 | 0.000000 | 0.000000 | 500 | 500 | 4.8606 | 0 |
| HARD_GATE | 5.0 | 0.000200 | 0.000025 | 0.002000 | 500 | 500 | 12.9168 | 1 |
| HARD_GATE | 10.0 | 0.001000 | 0.002279 | 0.010000 | 500 | 500 | 45.2748 | 5 |
| HARD_GATE | 20.0 | 0.004600 | 0.012769 | 0.046000 | 500 | 500 | 144.8336 | 23 |
| SHRINKAGE | 5.0 | 0.006600 | 0.017211 | 0.066000 | 500 | 500 | 205.3338 | 33 |
| SHRINKAGE | 10.0 | 0.016200 | 0.033562 | 0.142000 | 500 | 500 | 449.6004 | 81 |
| SHRINKAGE | 20.0 | 0.024000 | 0.046972 | 0.196000 | 500 | 500 | 737.5736 | 120 |
| SHRINKAGE | 50.0 | 0.033600 | 0.063728 | 0.258000 | 500 | 500 | 1078.0724 | 168 |
| HYBRID | 0.25 | 0.019000 | 0.044655 | 0.166000 | 500 | 500 | 636.0672 | 95 |
| HYBRID | 0.5 | 0.009000 | 0.023498 | 0.084000 | 500 | 500 | 290.3190 | 45 |
| HYBRID | 0.75 | 0.003000 | 0.008488 | 0.030000 | 500 | 500 | 98.6062 | 15 |
| POPULARITY | 0.0 | 0.039600 | 0.072640 | 0.316000 | 500 | 500 | 2893.3466 | 198 |

Hard gate giữ support >= 2/5/10/20 rồi xếp raw prediction DESC, movieId ASC.
Shrinkage dùng công thức, không clip score:

```text
reliability = train_core_support / (train_core_support + alpha)
adjusted_score = global_train_core_mean
                 + reliability * (als_score - global_train_core_mean)
```

Alpha = 5/10/20/50; sort adjusted_score DESC, movieId ASC.
Hybrid **RUN/PASS** cả lambda 0.25/0.50/0.75: normalized ALS/popularity ranks
trong cùng bounded unseen ALS pool; không cộng popularity counts thẳng vào
ALS score. Component = `1 - (rank - 1) / max_rank` theo từng user.

RAW ALS dùng raw top-100 candidate prefix để giữ baseline protocol trước;
rerankers dùng tối đa 1.000 ALS candidates/user. Popularity dùng 100 popular
eligible candidates/user, train_core-only counts. Factor/metadata eligibility
trong validation lấy từ **temporary model**. Popularity candidate list khác ALS;
bảng so sánh end-to-end ranking strategies trên cùng eligible item universe,
không phải cùng raw candidate list. Hybrid chỉ dùng cùng ALS candidate list.

Mỗi variant có đủ 5.000 recs, 500/500 users đủ 10; null/non-finite/duplicate
pairs/ranks/history overlap/metadata mismatch đều 0. Precision = macro hits/K;
recall = macro hits/relevant_count; HitRate = tỷ lệ user có hit. Users không
có recommendation vẫn được giữ trong metric. Independent pair-join đối chiếu
array intersection PASS cho cả 13 phương án. Không dùng RMSE để chọn reranker.

## 4. Selected config and immutable lock

**SHRINKAGE, alpha=50**, primary Recall@10.
Tie-break: Precision@10, HitRate@10, users đủ K, recommendation count, giải pháp
đơn giản hơn, parameter để bảo đảm deterministic. Popularity chỉ baseline,
không thuộc candidates có thể trở thành reranking config.

```text
selection_dataset = SAMPLE_VALIDATION_INDEPENDENT_OF_SELECTION_MODEL
support_source = SAMPLE_TRAIN_CORE_ONLY
candidate_pool_size = 1000
k = 10
seed = 42
```

Selected validation: **P@10=0.0336, R@10=0.06372782724594518, HR@10=0.258**.
Validation đã đạt quy tắc demo đặt trước test: recall gain >= 0.01,
precision cải thiện, số user đủ K không giảm. Quy tắc này là operating gate,
không phải kiểm định statistical significance.

Lock được ghi trước khi đọc test, không thay sau confirmation; SHA-256:

```text
edc14957c3f390cb29c0656dcb1d0afd8320abc6c129850c1c3130fb9ca14d7e
```

Lock chứa selection-model fingerprint, confirmation model path, params,
support/global mean và protected-output fingerprint. No overwrite.

## 5. Confirmatory SAMPLE test

Dùng **saved Phase B model**, không dùng temporary model để tạo final output.
Sample TV2 TEST đúng 315.869 rows của 10.000 saved sample users; metrics Top-10
trên 500 deterministic eligible test users theo cùng protocol Phase B/C.
Exclude toàn bộ sample train history; sample train-test pair overlap = 0.
Support/global mean giữ train_core-only, giống selection. Không fit hoặc retune.

| Method | P@10 | R@10 | HR@10 | Hits | Users with 10 | Mean core support |
|---|---:|---:|---:|---:|---:|---:|
| RAW_ALS | 0.000000 | 0.000000 | 0.000000 | 0 | 500 | 3.9766 |
| RERANKED_ALS | 0.090200 | 0.070510 | 0.454000 | 451 | 500 | 1037.7942 |
| POPULARITY | 0.125400 | 0.100374 | 0.588000 | 627 | 500 | 2869.5808 |

Mỗi method có 5.000 recs, 500 users, contract và independent metrics PASS.
RAW ALS P@10/R@10 vẫn **0/0**, không bị che giấu hoặc ghi đè.
Reranking giảm vấn đề phim ít support đứng đầu: mean core support của Top-10
đổi từ 3.9766 sang 1.037,7942. Reranked result cải thiện so với raw, nhưng chưa
vượt popularity ở cả validation và confirmation; không gọi ALS tốt nhất.

**TV2 TEST đã được quan sát ở Phase B/C. Kết quả Phase D là confirmatory,
không phải hoàn toàn untouched holdout.** Durable confirmation attempt được
ghi trước test read; verifier không lặp test evaluation. Config không đổi.

Popularity Phase D dùng core counts, khác Phase C dùng toàn bộ sample train;
627 hits so với 626 ở Phase C phản ánh baseline feature source khác, không phải
thay metric hoặc ghi lại kết quả Phase C. Saved ALS RMSE=0.8146154253125608,
MAE=0.6200873149304171 giữ nguyên.

## 6. Top-N reranked and TV4 handoff

**PASS / VERIFIED**: 20 deterministic saved demo users, **200 rows**, đủ Top-10.
Raw demo `recommendations/topn` giữ nguyên. Output mới:

```text
hdfs://namenode:9000/project/movielens/output/tv3/recommendations/topn_reranked
```

```text
userId INT, movieId INT, title STRING, genres STRING,
prediction DOUBLE, adjusted_score DOUBLE, train_support BIGINT, rank INT
```

Prediction là raw Phase B ALS score của candidate; adjusted_score là shrinkage
score dùng để xếp rank. Không clip prediction hoặc sửa raw artifacts.

| Verification | Kết quả |
|---|---:|
| Invalid/null/non-finite rows | 0 |
| History overlap | 0 |
| Duplicate user/movie | 0 |
| Duplicate user/rank | 0 |
| Unexpected users | 0 |
| Metadata mismatch | 0 |

Đã verify deterministic score/movieId ordering, contiguous ranks, support khớp
core counts, score khớp locked formula, exact Parquet read-back equality.
[TV4 handoff](TV3_HANDOFF_TO_TV4.md) được bổ sung sau khi verifier PASS. TV4 đọc
Parquet theo rank, không train hoặc chạy Phase D trong dashboard request.
Current Phase B manifest giữ `SAMPLE_COMPLETE_WARNING`; không sửa manifest cũ
để thêm reranked output, artifact mới được mô tả bởi Phase D reports/handoff.

## 7. Verifier and regression checks

**verification_status=PASS**, status=`PASS_CONFIRMATORY_TEST_LIMITATION`.
Selection-model independence PASS; fit calls=1; protected outputs unchanged;
test_evaluation_repeated=False. Compile/import **20 modules PASS** trong container;
compiled cache ở `/tmp`, source không bị ghi cache.

Regression PASS: shrinkage/zero support/no clipping, low-support demotion,
movieId tie ordering, core-only support, label-only zero support, original
split/partition determinism, history exclusion, user thiếu/không có recs,
independent pair-join metrics và selection tie-break/popularity exclusion.

Read-back verifier đối chiếu 13 validation rows và 3 test rows trong Parquet với
JSON, recompute selection từ saved validation results, kiểm tra config digest,
attempt, provenance, model fingerprints và demo contract/support/score.
Không fit lại model hoặc đo lại test ranking.

## 8. Artifacts and source changes

Paths dưới `/project/movielens/output/tv3/`, tất cả output mới, no overwrite:

| Artifact | Path |
|---|---|
| Temporary model | `diagnostics/reranking_phase_d/selection_model` |
| Fit attempt / model provenance | `diagnostics/reranking_phase_d/selection_fit_attempt`, `selection_model_report` |
| Regression result | `diagnostics/reranking_phase_d/regression_checks` |
| Validation report / config lock | `diagnostics/reranking_phase_d/validation_report`, `config_lock` |
| Validation user scope | `diagnostics/reranking_phase_d/validation_users` |
| Confirmation attempt / report | `diagnostics/reranking_phase_d/confirmation_attempt`, `test_report` |
| Test user scope | `diagnostics/reranking_phase_d/test_users` |
| Verification report | `diagnostics/reranking_phase_d/verification_report` |
| Validation metrics | `metrics/reranking_validation` |
| Test metrics | `metrics/reranking_test` |
| Reranked demo | `recommendations/topn_reranked` |

Added [support_reranking.py](../spark/tv3/support_reranking.py) và
[reranking_phase_d.py](../spark/tv3/reranking_phase_d.py); hoàn thiện protocol
model tạm theo quyền đã duyệt. Không sửa production training/Top-K modules.
Cập nhật báo cáo này, JSON evidence, README TV3 và TV4 handoff.

Đã chạy tuần tự `--stage checks`, `fit-selection`, `validation`, `confirm`,
`verify`. Các stages từ chối outputs đã tồn tại; không chạy lại để xem kết quả.
Các Docker preflight trước đó đã dừng đúng yêu cầu khi daemon không chạy;
không có fit/test attempt nào từ những lượt dừng đó.

## 9. Resources and runtime

Docker budget 8.184.639.488 bytes, khoảng 7,623 GiB. Mỗi lần một application;
driver 1g, 2 executors x 1536m/2 cores, tổng 4 cores, shuffle 64. Persist DISK_ONLY;
không collect/toPandas full ratings, không full user/movie cross join. ALS output
pool tối đa 500x1.000; popularity grid tối đa 500x100. Hybrid dùng lại ALS pool.

| Stage | Application ID | Master duration, giây |
|---|---|---:|
| Regression | app-20261005080352-0000 | 23.479 |
| Temporary fit/save/reload | app-20261005080443-0001 | 55.550 |
| Validation selection | app-20261005080603-0002 | 172.308 |
| Test confirmation/demo | app-20261005081905-0003 | 92.541 |
| Read-back verifier | app-20261005082109-0004 | 33.473 |

Tất cả exit 0 / FINISHED. Script validation runtime 164.401652 giây;
confirmation 83.785190 giây. Master duration gồm application lifecycle overhead.
Không có memory error/worker loss; không restart Docker/WSL/Hadoop hoặc đổi Compose.

Snapshot khi fit: workers 1,114/1,173 GiB, master 990,3 MiB; Linux MemAvailable
2.859.012 KiB (khoảng 2,73 GiB), swap không dùng. Đây là snapshot, không phải
peak liên tục. Sau tất cả jobs: workers 145,3/146,2 MiB, master 327,9 MiB;
2 workers ALIVE, 0 active applications.

## 10. HDFS and artifact protection

**HDFS HEALTHY**, 3/3 DataNodes, Safe Mode OFF. Final fsck: 288 files,
250 validated blocks; missing/corrupt/under-replicated blocks = 0,
missing replicas = 0, average replication = 3.0.

47 protected Phase B/C files giữ nguyên path/size/mtime/HDFS file checksum;
fingerprint SHA-256 trước/sau fit, selection, confirmation và verifier:

```text
a27fce31e478a4d2ab3fc05f9f93fa7de03701489d4dbb9c723f4744a6b9c186
```

Bao gồm Phase B model/final metrics/raw Top-K/raw demo/manifest/scopes,
A/B/C evidence/tuning manifest và Phase C diagnostic. Temporary model có
24 files, fingerprint riêng được khóa và kiểm tra lại:

```text
cb81fd3524ac09fe69aeedbf093530b3e867640c44c349ac125bf74a0b2e7788
```

Không xóa model, volumes hoặc artifacts cũ. TV1/TV2 không sửa/chạy lại.

## 11. Git

`git diff -- spark/tv1 spark/tv2`: **empty**.
`git diff --check`: **PASS**. Shared `docker-compose.yml` và
`docker-compose.tv3.yml` không đổi. Không commit/push/merge.
Các file dự thảo Phase D từ lượt chuẩn bị được hoàn thiện; không xóa/hoàn nguyên
thay đổi ngoài phiên.

## 12. Completion and next recommendation

Temporary selection model: **PASS**. Validation/lock: **PASS**.
Sample test confirmation: **PASS**. Reranked Top-N: **PASS**.
Verifier: **PASS**. HDFS: **HEALTHY**.
**READY TO CLOSE TV3 SAMPLE: YES**, giữ giới hạn sample/confirmatory và cảnh báo
popularity vẫn tốt hơn. Improvement so với raw ALS có ý nghĩa theo quy tắc
validation đã đặt: từ 0 lên 451 test hits, 45,4% test users có hit; chưa có
confidence intervals hoặc phép kiểm định statistical significance.

**Full ALS readiness vẫn NO theo resource evidence hiện có**; thành công sample
không chứng minh full train 25,5M rows vừa RAM. Nếu có phạm vi tiếp theo, ưu tiên
đánh giá ranking/coverage rộng hơn bằng validation và giữ popularity reference.
Không tự mở thêm experiment hoặc đổi config sau test. **Dừng sau Phase D.**
