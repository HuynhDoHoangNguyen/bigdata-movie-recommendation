# TV3 Phase C — Ranking quality diagnostic

Ngày hoàn tất: 05/10/2026 (Asia/Saigon). Trạng thái diagnostic: **DIAGNOSTIC_COMPLETE**.
Handoff giữ nguyên **SAMPLE_COMPLETE_WARNING**.

Đã kiểm tra saved SAMPLE model trên cùng 500 deterministic eligible users của
Phase B, không fit lại. Không phát hiện bug trong metric, ID matching hoặc
history/metadata filtering đã kiểm tra. Kết quả cho thấy ranking ALS của sample
này kém: điểm cao tập trung vào phim có ít dữ liệu train, còn phần lớn relevant
test items có factors nằm ngoài các vị trí đầu. Popularity train-only có hit
trên cùng tập đánh giá, nên sự thưa của exact-match không đủ giải thích 0 hit.

Evidence đầy đủ, chưa làm tròn: [TV3_RANKING_DIAGNOSTIC_RESULTS.json](TV3_RANKING_DIAGNOSTIC_RESULTS.json).
Diagnostic source: [ranking_diagnostic.py](../spark/tv3/ranking_diagnostic.py).
Kết quả chỉ mô tả saved SAMPLE model và 500 user đã chọn, không đại diện cho full ALS.

## 1. Original Precision@10 / Recall@10

| Phase B artifact, giữ nguyên | Giá trị |
|---|---:|
| Model users / train ratings | 10.000 / 1.243.712 |
| Params | rank=10, maxIter=8, regParam=0.05, seed=42 |
| Final sample RMSE / MAE | 0.8146154253125608 / 0.6200873149304171 |
| Evaluation users / relevant user-movie pairs | 500 / 8.569 |
| Recommendations / total hits | 5.000 / 0 |
| Macro Precision@10 / Recall@10 | **0.0 / 0.0** |

Relevance lấy từ TV2 TEST sau khi giới hạn bằng 10.000 saved sample user IDs,
`rating >= 4.0`; không lấy từ train hoặc validation. Evaluation users chọn bằng
`xxhash64(userId, lit(42))`, tie-break userId, limit 500 từ users có relevance,
giống helper Phase B. Tất cả 500 user đều có saved model user factors.

## 2. Metric implementation audit

Đã đọc toàn bộ `topn.py`, đối chiếu `sample_handoff.py`, các entrypoint
training/tuning/evaluation TV3 và original TV2 split source theo chế độ read-only.

| Kiểm tra | Bằng chứng / kết luận |
|---|---|
| Relevant threshold và input | SAMPLE TV2 TEST, rating >= 4.0; PASS |
| Evaluation user factors | 500/500 có factors; PASS |
| Hit identity | Join `(userId, movieId)`, không dùng title; PASS |
| ID types | Recommendation và relevance đều movieId INT; PASS |
| History exclusion | Left-anti train pairs; Top-10/history overlap = 0 |
| Relevant items bị lọc nhầm | Relevant/history overlap = 0; pool relevant counts không giảm sau filtering |
| Duplicate recommendations | Top-10 duplicate user/movie pairs = 0 |
| Precision denominator | Mỗi user: hits / K; macro average trên 500 user |
| Recall denominator | Mỗi user: hits / số relevant test items; macro average trên 500 user |
| Zero-hit / zero-recommendation users | Independent audit giữ user bằng left joins, fill missing counts bằng 0 |
| Candidate metadata filter | Missing metadata candidates = 0 ở cả 4 K |
| Test leakage | Training/tuning đọc train/validation; sample train-test pair overlap = 0; baseline chỉ dùng sample train |

Independent pair-join metric tái hiện Precision@10/Recall@10 bằng 0 của artifact
Phase B. Synthetic nonzero fixture chạy **helper gốc** với 2 user, 2 hits,
4 recommendations sau history filtering: Precision@10 = 0.1, Recall@10 = 0.75,
đúng giá trị biết trước. Fixture không fit ALS.

Điểm recommendation và `model.transform` trên cùng 5.000 user/movie pairs khớp:
max absolute difference = 1.430511474609375e-06, số sai khác > 1e-4 = 0.
Không phát hiện lỗi production cần sửa; không sửa helper hoặc ghi lại metrics cũ.

## 3. Candidate generation audit

Saved ALS sinh một bounded raw pool tối đa 1.000 items/user cho 500 user.
Diagnostic lấy prefix `10*K`, loại train history, xếp score giảm dần rồi movieId
tăng dần và lấy K, cùng chiến lược buffer của helper gốc. Không cross join
10.000 users × toàn bộ movies.

| K | Raw pool/user | Tổng raw | Sau history filter | Trung bình sau filter/user | Relevant pairs raw → sau filter | Thiếu metadata |
|---:|---:|---:|---:|---:|---:|---:|
| 10 | 100 | 50.000 | 49.825 | 99.650 | 24 → 24 | 0 |
| 20 | 200 | 100.000 | 99.465 | 198.930 | 87 → 87 | 0 |
| 50 | 500 | 250.000 | 247.757 | 495.514 | 445 → 445 | 0 |
| 100 | 1.000 | 500.000 | 494.198 | 988.396 | 1.112 → 1.112 | 0 |

Không thiếu candidates để trả đủ K: cả 500 user có đủ K recommendations ở
mọi K đã đo. Filtering không làm mất relevant pairs hiện diện trong raw pool.
Raw top-100 chỉ chứa 24/8.503 relevant pairs có factors (0,2823%); raw top-1.000
chứa 1.112/8.503 (13,0777%). Đây là hạn chế của score ordering/candidate coverage,
không phải pool bị cạn vì history filter. Tăng buffer không bảo đảm cải thiện
Top-10, vì các phim có score cao vẫn đứng trước các relevant items có score thấp.

## 4. Relevant-item eligibility

| Phạm vi | Số lượng |
|---|---:|
| Saved model item factors | 27.941 |
| Unique relevant movies trong toàn bộ sample test | 10.533 |
| Unique relevant sample-test movies có item factors | 9.893 |
| Unique relevant sample-test movies thiếu factors | 640 |
| Unique relevant movies của 500 evaluation users | 3.089 |
| Relevant user/movie pairs của 500 users | 8.569 |
| Relevant pairs có / thiếu item factors | 8.503 / 66 |
| Known relevant pairs ngoài raw top-100 | 8.479 |
| Known relevant pairs ngoài raw top-1.000 | 7.391 |

Thiếu factors chỉ ảnh hưởng 66/8.569 relevant pairs (0,7702%) trong tập đánh giá;
không giải thích việc toàn bộ Top-10 không hit. Denominator recall giữ tất cả
relevant test items, kể cả cold-start items, để không nâng metric bằng cách
thay đổi eligibility definition. Phân biệt số unique movie với số user/movie pairs.

## 5. Results at K=10/20/50/100

Precision, Recall và HitRate là macro/user; HitRate là tỷ lệ user có ít nhất
một hit. Recall không phải total_hits / 8.569. Số dưới đây làm tròn để trình bày.

| K | Precision@K | Recall@K | HitRate@K | Total hits | Recommendations | Users thiếu K |
|---:|---:|---:|---:|---:|---:|---:|
| 10 | 0.000000 | 0.000000 | 0.000 | 0 | 5.000 | 0 |
| 20 | 0.000400 | 0.000186 | 0.008 | 4 | 10.000 | 0 |
| 50 | 0.000280 | 0.000720 | 0.014 | 7 | 25.000 | 0 |
| 100 | 0.000480 | 0.004227 | 0.040 | 24 | 50.000 | 0 |

Mở rộng K tạo một số hits nhưng ranking vẫn rất yếu; không còn chính xác bằng
0 từ K=20, song chỉ 4% users có hit tại K=100. Chưa có bằng chứng để coi việc
tăng K là giải pháp chất lượng.

## 6. Train-only popularity baseline

Baseline PASS. Popularity = số rating/movie trong cùng 1.243.712 sample TV2
train rows dùng để fit saved model. Tie-break movieId tăng dần. Không sử dụng
test ratings hoặc relevant labels để tính popularity.

Cùng 500 users, relevance, train-history exclusion, model item-factor/metadata
eligibility và K với ALS. Buffer cũng là `10*K`, nhưng ordering theo popularity,
nên tập candidate cụ thể khác ALS; đây là so sánh hai ranking strategies trên
cùng eligible universe, không phải cùng raw candidate list. Baseline chỉ cross
join 500 users × tối đa 1.000 popular eligible items (500.000 pairs).

| K | Precision@K | Recall@K | HitRate@K | Total hits | Users thiếu K |
|---:|---:|---:|---:|---:|---:|
| 10 | 0.125200 | 0.100133 | 0.588 | 626 | 0 |
| 20 | 0.097900 | 0.154268 | 0.690 | 979 | 0 |
| 50 | 0.066800 | 0.245753 | 0.810 | 1.670 | 0 |
| 100 | 0.047000 | 0.335301 | 0.878 | 2.350 | 0 |

Baseline không thay thế ALS/model/demo hiện có và không được xuất sang output
recommendation Phase B. Kết quả cho thấy exact-match evaluation có thể ghi nhận
hits; sparsity không đủ giải thích mức chênh lệch quan sát được.

## 7. Controlled case studies

Chọn deterministic 5 user từ chính 500 evaluation users bằng cùng hash/seed,
trình bày theo userId. Scores bên dưới làm tròn 6 chữ số; JSON giữ giá trị gốc.
Không dump toàn bộ rating dataset.

### User 54836

Train history: **43**; relevant test: **4**; relevant có factors: **4**; Top-10 intersection: **0**.

Relevant movieIds: `5816, 5995, 54001, 69844`.

| Rank | Top-10 movieId | ALS score |
|---:|---:|---:|
| 1 | 213668 | 6.817441 |
| 2 | 66385 | 5.512827 |
| 3 | 169882 | 5.400080 |
| 4 | 62586 | 5.386717 |
| 5 | 221570 | 5.385686 |
| 6 | 69241 | 5.329673 |
| 7 | 88138 | 5.320153 |
| 8 | 181873 | 5.278503 |
| 9 | 187697 | 5.278503 |
| 10 | 25738 | 5.274997 |

Relevant trong raw top-100 / top-1.000: **0 / 0**. Cả 4 relevant items có factors nhưng đều ngoài raw top-1.000; không phải cold-start hoặc history filtering.

### User 103269

Train history: **44**; relevant test: **8**; relevant có factors: **8**; Top-10 intersection: **0**.

Relevant movieIds: `1197, 1252, 2115, 2968, 4993, 5952, 6539, 6947`.

| Rank | Top-10 movieId | ALS score |
|---:|---:|---:|
| 1 | 169882 | 7.026616 |
| 2 | 142861 | 6.527352 |
| 3 | 210105 | 6.331396 |
| 4 | 27587 | 6.304311 |
| 5 | 201450 | 6.264384 |
| 6 | 88138 | 6.225275 |
| 7 | 7578 | 6.174534 |
| 8 | 126086 | 6.108622 |
| 9 | 7959 | 6.101423 |
| 10 | 103210 | 6.072297 |

Relevant trong raw top-100 / top-1.000: **0 / 0**. Cả 8 relevant items có factors nhưng ngoài raw top-1.000; cả 10 recommended scores đều trên 6.

### User 108505

Train history: **92**; relevant test: **9**; relevant có factors: **9**; Top-10 intersection: **0**.

Relevant movieIds: `1198, 3793, 4963, 33794, 49272, 65216, 89085, 109374, 134130`.

| Rank | Top-10 movieId | ALS score |
|---:|---:|---:|
| 1 | 213668 | 5.792048 |
| 2 | 72037 | 5.492245 |
| 3 | 104350 | 5.336960 |
| 4 | 53827 | 5.143733 |
| 5 | 210105 | 5.113659 |
| 6 | 216845 | 5.062499 |
| 7 | 182637 | 5.032284 |
| 8 | 202359 | 5.023951 |
| 9 | 7456 | 4.958004 |
| 10 | 5352 | 4.943212 |

Relevant trong raw top-100 / top-1.000: **0 / 0**. Cả 9 relevant items có factors nhưng ngoài raw top-1.000; ALS score ordering không đưa chúng vào pool.

### User 128517

Train history: **65**; relevant test: **13**; relevant có factors: **13**; Top-10 intersection: **0**.

Relevant movieIds: `232, 318, 904, 1203, 3578, 72226, 91077, 97938, 99149, 160980, 171763, 177615, 207313`.

| Rank | Top-10 movieId | ALS score |
|---:|---:|---:|
| 1 | 169882 | 6.373469 |
| 2 | 201450 | 6.033068 |
| 3 | 151279 | 5.630286 |
| 4 | 142861 | 5.595545 |
| 5 | 128151 | 5.501084 |
| 6 | 58898 | 5.494441 |
| 7 | 126941 | 5.494441 |
| 8 | 196787 | 5.494441 |
| 9 | 250764 | 5.494441 |
| 10 | 210105 | 5.452452 |

Relevant trong raw top-100 / top-1.000: **1 / 3**. Có 1 relevant item trong raw top-100 và 3 trong raw top-1.000, nhưng không item nào vào Top-10; pool omission không phải lời giải thích duy nhất.

### User 143637

Train history: **116**; relevant test: **18**; relevant có factors: **18**; Top-10 intersection: **0**.

Relevant movieIds: `104, 216, 318, 1060, 1265, 1466, 2502, 2706, 2707, 2762, 3386, 3617, 4011, 4022, 6188, 6440, 7147, 7160`.

| Rank | Top-10 movieId | ALS score |
|---:|---:|---:|
| 1 | 171179 | 6.190204 |
| 2 | 128089 | 6.024666 |
| 3 | 4208 | 5.894129 |
| 4 | 142861 | 5.719175 |
| 5 | 86347 | 5.606514 |
| 6 | 90592 | 5.595092 |
| 7 | 79477 | 5.590473 |
| 8 | 69241 | 5.580616 |
| 9 | 201450 | 5.578479 |
| 10 | 121155 | 5.551732 |

Relevant trong raw top-100 / top-1.000: **0 / 1**. Cả 18 relevant items có factors; chỉ 1 nằm trong raw top-1.000 và không item nào trong raw top-100.

## 8. Identified cause and unresolved hypotheses

| Nhóm nguyên nhân | Kết luận trong phạm vi đã kiểm tra |
|---|---|
| A. Metric implementation bug | Không có bằng chứng: synthetic nonzero fixture PASS, independent metric khớp |
| B. Candidate-generation limitation | Có coverage yếu do ALS score ordering; không bị cạn pool, không mất relevance qua filtering |
| C. Eligibility / ID mismatch | INT/INT và 500 user factors đúng; item cold-start 0,7702% relevant pairs là giới hạn nhỏ |
| D. Exact-match quá thưa | Có tác động nhưng không đủ giải thích: baseline cùng protocol có 626 Top-10 hits |
| E. ALS ranking thực sự kém | Được xác nhận trên saved SAMPLE model / 500-user evaluation này |

Cơ chế quan sát được: **3.937/5.000 recommendations (78,74%)** đến từ items có
tối đa 5 sample train ratings; trung bình train support của recommended item
là 4,4224. Chỉ 588 unique movies xuất hiện trong 5.000 Top-10 rows.
Recommended scores trung bình 5,748091, khoảng 3,334376–10,028754; predictions
trên known relevant test rows trung bình 3,849211, khoảng -1,547890–5,669611.
Explicit ALS score không bị chặn trong rating range. Việc score API khớp loại
trừ lỗi đọc recommendation score trong diagnostic.

Phim ít train support có điểm cao chiếm những vị trí đầu, đẩy relevant items
xuống ngoài Top-10 và thường ngoài top-1.000. Đây là bằng chứng về ranking
failure, không chứng minh một hyperparameter cụ thể gây ra lỗi hoặc một thay
đổi cụ thể chắc chắn sửa được. Việc chọn model theo validation RMSE và objective
explicit rating prediction thay vì ranking là các giả thuyết góp phần; mức ảnh
hưởng cần thử nghiệm validation riêng. Không quy kết TV1/TV2 preprocessing.

## 9. Recommendations for later ranking work

Các hướng sau là đề xuất cho phạm vi tiếp theo, chưa áp dụng hoặc fit lại:

1. Đo Precision/Recall/HitRate và train-support distribution trên held-out
   validation từ TV2 train; chọn cải tiến bằng validation, không dùng TV2 test
   đã quan sát để retune rồi báo lại như một independent final test.
2. Thử candidate reliability gate/penalty dựa trên train support; cân nhắc
   popularity candidates kết hợp ALS reranking. Kiểm tra coverage và ảnh hưởng
   long-tail, không mặc định bỏ mọi phim hiếm là tốt hơn.
3. Nếu được giao thêm phạm vi, thử regularization hoặc score/reliability
   calibration trên sample và so sánh ranking metrics với popularity baseline.
4. Nếu mục tiêu là implicit ranking, đánh giá một experiment riêng với implicit
   objective phù hợp, giữ nguyên split và artifacts hiện có để so sánh.

Không sửa metric để làm đẹp số, không clip score rồi tuyên bố đã sửa ranking,
không tự chuyển sang Full ALS. TV4 tiếp tục dùng output hiện tại cho demo với
cảnh báo chất lượng đã ghi trong handoff.

## 10. Source changes and verification

Phase C thêm `spark/tv3/ranking_diagnostic.py`, báo cáo này và JSON evidence;
cập nhật README TV3 và tài liệu handoff để dẫn tới kết quả diagnostic.
Không sửa production Top-K/training modules trong Phase C.
Diagnostic kiểm tra output mới chưa tồn tại và chỉ ghi riêng:

```text
hdfs://namenode:9000/project/movielens/output/tv3/diagnostics/ranking_phase_c
```

Lần chạy diagnostic đầu gặp Spark `AnalysisException` về ambiguous factor
self-join khi đưa DataFrame đã join itemFactors vào `model.transform`; dừng
không ghi output. Chỉ sửa diagnostic: dự đoán trên test rows chưa join factors,
và tách lineage của bounded recommendation frame qua distributed RDD trước
score consistency check. Không tắt Spark ambiguity guard hoặc đổi source TV1/TV2.
Lần chạy sau exit 0; synthetic fixture, metric agreement, score agreement và
artifact fingerprint đều PASS. Đây là lỗi diagnostic đã khắc phục, không phải
bug của metric production.

Final compile/import của source diagnostic PASS; `git diff --check` PASS.
`git diff -- spark/tv1 spark/tv2` không có output. Không ghi đè model, metrics,
Top-N demo, tuning evidence hoặc handoff manifest; không commit/push.
Các thay đổi có sẵn từ trước Phase C được giữ nguyên.

## 11. Resource usage

Docker memory limit khoảng 7,623 GiB. Mỗi lần chỉ một Spark application;
driver 1g, 2 executors × 1536m, 2 cores/executor, tổng 4 cores, shuffle 64.
Persist diagnostic frames bằng DISK_ONLY; không collect/toPandas full ratings.
Driver chỉ nhận aggregates và 5 bounded case studies. Raw diagnostic pool và
baseline grid mỗi loại tối đa 500.000 pairs, không có full user/movie cross join.

Successful diagnostic runtime: **128,312348 giây**, exit 0. Lần đầu dừng vì
AnalysisException, không phải memory error. Không có OOM/worker loss, không
restart Docker/WSL/Hadoop, không sửa Compose hoặc xóa volumes.

Snapshot khi job chạy: Spark master khoảng 1,605 GiB, workers 1,501 và 1,392 GiB;
Linux MemAvailable 1.605.628 KiB (khoảng 1,53 GiB). Đây là snapshot quan sát,
không phải đo peak liên tục. Sau job workers còn khoảng 156,5/164,5 MiB,
master 282,2 MiB; 2 workers alive, 0 active applications.

## 12. HDFS integrity and protected outputs

Sau diagnostic: HDFS **HEALTHY**, 3/3 DataNodes, safe mode OFF; fsck kiểm tra
238 files / 216 validated blocks, missing/corrupt/under-replicated blocks = 0,
missing replicas = 0, average replication = 3.0.

Fingerprint của 43 protected HDFS files gồm saved model, final metrics, Top-K
metrics, demo recommendations, experiments, manifest, sample user scope và
tuning manifest khớp trước/sau. Fingerprint tổng hợp path, length, modification
time và HDFS file checksum; SHA-256 cả hai lần:

```text
46ff7632721e6639ec059428effaeb5429a2bfbc1c33186bfc80c09d5c7c0ead
```

Diagnostic JSON được ghi riêng (10.018 bytes) ở đường dẫn mới trên; bản local
trong docs giữ nội dung với formatting dễ đọc. Không chỉnh hoặc chạy lại
TV1/TV2. Phase C kết thúc ở diagnostic, trạng thái SAMPLE handoff không đổi.
