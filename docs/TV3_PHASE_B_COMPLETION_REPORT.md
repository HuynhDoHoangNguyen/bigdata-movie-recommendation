# TV3 PHASE B COMPLETION REPORT

Ngày: 05/10/2026 (Asia/Saigon).
**SAMPLE_COMPLETE_WARNING — bộ deliverables SAMPLE đã hoàn tất và verify PASS.**
Không chạy full ALS hoặc tuning A/B/C lại. Các con số sau là runtime máy mới.

## 1. Sample model status

Entry point riêng `spark/tv3/final_sample.py`, không có full-data option.
Đọc candidate từ A/B/C đã persist và đối chiếu selection Phase A:
rank=10, maxIter=8, regParam=0.05, seed=42. Train đúng một model.
Sampling tái sử dụng helper/strategy gốc; persisted 10.000 user khớp tập tuning.
Final model fit bao gồm train-core và validation cũ, không chỉ train-core.

## 2. Training rows

Sample users: **10.000**. Training rows: **1.243.712**.
Fit runtime: **9.18632078399969 giây**.
App `app-20261004173803-0010` FINISHED, application duration 45,025 giây
(gồm input checks, fit, save/reload, test evaluation).

## 3. Model HDFS path

`hdfs://namenode:9000/project/movielens/output/tv3/model/als_best`

## 4. Model reload result

PASS trong training job và độc lập ở handoff/verifier applications.
10.000 user factors, 27.941 item factors. Các ID unique; invalid factor rows 0.
Factor arrays đúng rank, không null/NaN/Infinity. Independent verifier còn
đối chiếu tập user/item factor IDs với sampled train, không chỉ đếm rows.
ALSModel persist rank/cold-start settings; maxIter/regParam/seed được kiểm tra
qua provenance từ tuning manifest và final metrics, không giả định ALSModel
giữ các estimator-only params này.

## 5. Final sample test rows

**315.869** TV2 test rows của đúng 10.000 model users. Không dùng validation
tuning để báo final test metrics; không thay đổi TV2 test hoặc split.

## 6. Valid/dropped predictions

- Valid: **313.351**.
- Dropped: **2.518**, **0.7971659137173955%**.
- Cold-start sample: 0 unknown users, 2.337 unknown movies.
- Affected test rows=2.518, khớp dropped count.

Cold-start khác full TV2 statistics vì model chỉ thấy sampled training items;
không ép số này khớp 4.178 movies/4.729 rows của full TV2 split.

## 7. Final RMSE/MAE

RMSE **0.8146154253125608**, MAE **0.6200873149304171**.
Rating evaluation runtime **2.478176572999473 giây**.
Scope: SAMPLE; evaluation_dataset: TV2 test deterministic user sample.
Metrics đã persist tại `/project/movielens/output/tv3/metrics/final`, được
đọc lại và đo lại từ saved model trong verifier; kết quả khớp.

## 8. Top-K results / warning

Precision@10=**0.0**, Recall@10=**0.0**, threshold rating>=4.0.
Scope: 500 deterministic eligible users trong sample. Có 9.879 sample eligible
users; không phải full MovieLens. Runtime ban đầu **8.638682 giây**.
Metrics tại `/project/movielens/output/tv3/metrics/top_k`.

Vì kết quả bằng 0, đã kiểm tra lại độc lập: 5.000 recommendations, 8.569 relevant
items, total_hits=0, users_with_hits=0; output không rỗng. Giữ nguyên metric thật.
Đây là hạn chế chất lượng ranking, chưa xác định root cause trong Phase B.
Không gọi output demo là recommender đã có chất lượng ranking tốt.

## 9. Top-N result

`/project/movielens/output/tv3/recommendations/topn`: **200 dòng**, 20 user,
Top-10/user, schema đúng userId int/movieId int/title string/genres string/
prediction double/rank int. Null/non-finite predictions, duplicate pairs,
duplicate ranks, history overlap, unexpected users và metadata mismatches đều 0.
Demo IDs và dataset user scopes được bàn giao trong tài liệu TV4.

Job riêng `sample_handoff.py`, app `app-20261004173917-0011`, FINISHED,
duration 41,242 giây. Không fit hoặc tune model trong job này.

## 10. Verifier / Manifest

`verify_outputs.py` hỗ trợ SAMPLE contract qua `verify_sample.py`:
`verification_status=PASS`, `status=WARNING`, `errors=[]`.
App `app-20261004174124-0012` FINISHED, duration 45,378 giây.

Verifier đọc lại model, factors, sampled users, sampled train/test, final
metrics, recommendations, movies, Top-K và tuning evidence. Kiểm tra scope,
counts, factor ID sets, coverage, finite metrics, history/metadata, A/B/C
equality; đo lại rating metrics và ranking metrics không fit/tune lại.
Manifest tại `/project/movielens/output/tv3/manifest`:

```text
status = SAMPLE_COMPLETE_WARNING
model_scope = SAMPLE
evaluation_scope = SAMPLE
recommendation_scope = SAMPLE_DEMO
```

Tuning experiment Parquet giữ nguyên. Phase A manifest được lưu tại
`/project/movielens/output/tv3/audit/tuning_manifest` và nhúng trong current
manifest. Không mất evidence A/B/C khi thay current manifest bằng handoff.

## 11. TV4 handoff

[TV3_HANDOFF_TO_TV4.md](TV3_HANDOFF_TO_TV4.md): paths, schema, actual metrics,
20 demo user IDs, scope, limitations và ví dụ đọc Parquet. TV4 chỉ đọc output
đã materialize; không train ALS trong dashboard request.

## 12. HDFS health / resource

Sau Phase B: HEALTHY, 3/3 DataNodes, safe mode OFF; 236 files, 215 validated
blocks, missing/corrupt/under-replicated blocks 0, missing replicas 0,
average replication 3.0. Cuối runtime checks: 2 workers ALIVE, active apps 0.

Các jobs tuần tự, driver 1 GiB, 2 executors × 1.536 MiB × 2 cores,
shuffle partitions 64. Không ghi nhận memory error/worker loss.
Snapshot training: worker1 1,072 GiB, worker2 894 MiB, master+driver 899,7 MiB,
Docker/WSL MemAvailable khoảng 3,12 GiB. Đây không phải peak đo liên tục.
Docker RAM vẫn 7,62 GiB. Sau handoff MemAvailable khoảng 5,47 GiB.
Ổ D còn khoảng 148,12 GiB; Docker virtual filesystem khoảng 945 GiB available.
Không restart Docker/WSL hoặc containers, không đổi Compose.

## 13. Git / source protection

Chỉ tạo/sửa source TV3 và tài liệu TV3 trong Phase B:

- Added: `final_sample.py`, `sample_handoff.py`, `output_contract.py`, `verify_sample.py`.
- Updated: `config.py` (TV3 artifact paths), `recommend.py` (write boundary,
  no overwrite, saved-model user membership), `verify_outputs.py` (SAMPLE branch),
  `topn.py` (ranking hit/coverage diagnostics, không đổi thuật toán metric).
- Docs: report này, handoff, README, banner issues report cũ.

Compile/import 17 modules PASS. Path boundary tests PASS, gồm từ chối TV1/TV2,
traversal, wrong scheme/authority, và CLI ghi vào tuning metrics. CLI path
rejection xảy ra trước Spark startup. User -1 ngoài model bị từ chối đúng với
exit code 1; HDFS probe output không tồn tại. Đây là negative test có chủ ý.

`git diff -- spark/tv1 spark/tv2`: **empty**.
`git diff --check`: **PASS**. Dataset line-ending warnings có từ trước, giữ nguyên.
Không commit/push/merge, không revert/reset/clean; mọi thay đổi trước Phase B
được bảo toàn. Git status có các source/docs TV3 mới/sửa và các thay đổi cũ:
hai dataset docs, backup data directory, untracked Compose TV3, recovery/audit
docs và input_gate. Không đưa dataset CSV/HDFS outputs vào repo.

## 14. Remaining limitations / checkpoint

Phase B hoàn tất theo phạm vi SAMPLE được yêu cầu. Full-data ALS chưa train,
readiness vẫn NO; không tự chuyển sang full. Dashboard chỉ có recommendation
materialized cho 20 demo users, không phải toàn bộ 10.000 model users.
RMSE/MAE chỉ phản ánh predictions hợp lệ, đã drop 0,797166% sample test rows.
Ranking metrics 0 cần công việc chất lượng riêng nếu muốn mở rộng demo.
Candidate buffer hữu hạn; ALS score không phải xác suất hoặc rating đã clip.
Chưa commit/push theo yêu cầu; repository packaging cần review riêng.
**DỪNG tại Phase B.**
