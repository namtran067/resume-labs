# Part 5 — Interview Playbook: câu chốt, map CV, drill tự kiểm chứng

> Nguyên tắc: lý thuyết Part 0–4 chỉ có giá trị khi **neo vào chuyện thật đã làm**. File này ánh xạ mỗi chủ đề vào CV `cv/data-engineer-jemmia/` để trả lời dạng *"anh/chị đã dùng nó thế nào?"*

## 1. Câu chốt 30 giây mỗi chủ đề (thuộc lòng, nói không cần nghĩ)

| Chủ đề | Câu chốt |
|---|---|
| HDFS | "Block 128MB + replica x3 đặt theo rack awareness; NameNode giữ metadata trong RAM nên small files là gián điệp phá cluster." |
| S3 | "Không rename, không append, không atomic multi-object → không ACID tại storage → table format ra đời để bù." |
| Delta | "Parquet cộng `_delta_log` — cuốn sổ cái JSON biến object storage thành bảng transactional." |
| Iceberg | "Metadata tree cộng hidden partitioning — query theo `event_ts` là tự partition filter, đổi partition spec không phá data cũ." |
| Hudi | "Timeline + record-level index — sinh ra để upsert gần real-time." |
| COW/MOR | "COW trả công viết mua đọc nhanh; MOR ngược lại và trả bằng compaction." |
| Batch vs Stream | "Chọn theo latency requirement thực — dashboard 5 phút vẫn là micro-batch, đừng stream hoá mọi thứ." |
| Watermark | "Cam kết đóng cửa sổ event-time — dài thì chính xác tốn state, ngắn thì nhanh mất event trễ." |
| Exactly-once | "At-least-once + sink idempotent/transactional; engine chỉ đảm bảo trong nội bộ." |
| Parquet | "Row group + column chunk + footer stats: chỉ đọc đúng cột, đúng page, bỏ qua cả row group." |
| Avro | "Row-based + schema JSON tách rời + single-object encoding cho Kafka — vua của ingestion door." |
| Spark tuning | "Plan trước, partitioning sau, serialization Kryo, cuối cùng mới boost memory — Spark UI chẩn đoán, không đoán mò." |

## 2. Map kiến thức vào CV — bảng chuyển hoá trải nghiệm

| Chủ đề lý thuyết | Chốt câu chuyện trong CV | Câu trả lời mẫu (khung STAR rút gọn) |
|---|---|---|
| S3/object storage + table format | **Mini Lakehouse lab (2026)**: Iceberg + Spark + Trino + Polaris REST catalog trên **MinIO** | "Tôi xây lakehouse đa engine trên MinIO — S3-compatible nên gặp đúng các đặc tính flat namespace/atomicity của object store; commit atomic do Polaris giữ con trỏ metadata." |
| Object storage production | **Jemmia**: PIM trên **Cloudflare R2** (data lake, 40+ bảng NocoDB, backfill 8,000+ SKU zero-downtime + CDC) | "R2 là object storage nên backfill của tôi thiết kế idempotent — chạy lại không sinh trùng, vì coi mỗi lần ghi là upsert theo SKU." |
| Iceberg partition/maintenance | **Mini Lakehouse**: partitioning, time travel, COW/MOR, table maintenance end-to-end | "Tôi dùng `days()` partition, verify time travel bằng query as-of snapshot cũ, đo khác biệt COW/MOR khi update — hiểu cả hai bằng tay chứ không chỉ đọc docs." |
| Batch pipeline + volume | **Jemmia**: Dagster software-defined assets, dlt/Airbyte/n8n, 12+ nguồn, 150 raw tables, **500K+ rows/day**; dbt 200+ models | "500K dòng/ngày là bài toán batch thuần — tôi tối ưu theo hướng partition + freshness tests của dbt thay vì over-engineer streaming." |
| Event-driven / streaming nhỏ | **Jemmia RFID**: webhook từ scanner phần cứng → AWS Lambda, ~1,000 records/3MB mỗi scan, 20 scans/ngày (~20K records/day) | "Đó là mini-streaming: webhook at-least-once + Lambda idempotent theo `scan_id`; sự kiện có thể đến trễ/out-of-order nên tôi chuẩn hoá theo event-time tại scanner — đúng bài toán watermark đã thuộc, chỉ ở quy mô nhỏ." |
| Orchestration / data quality | Dagster SDA + dbt tests (schema, not-null, relationships, freshness) + OpenMetadata lineage | "Materialization của Dagster cho tôi lineage + retry semantics giống checkpoint của stream engine." |
| Row vs columnar | Parquet trong skills + minilakehouse; DuckDB/StarRocks/BigQuery analytics | "Bronze giữ raw, silver/gold Parquet — chính là 'Avro at the door, Parquet at the table' với codec zstd/snappy." |

### Bridge các khoảng trống (nói thật + chủ động)

- Chưa vận hành Kafka/Iceberg ở production quy mô lớn → *"Tôi hiểu sâu cơ chế qua lab end-to-end và maps trực tiếp từ trải nghiệm R2/CDC tại Jemmia; bước tiếp theo của tôi chính là đưa pattern này vào production."*
- Hadoop/HDFS chỉ lý thuyết → neo qua MinIO + hiểu ancestor: *"Block/replica là gốc của mọi phân mảnh + partition hiện đại."*

## 3. Drill tự kiểm chứng (checklist thực hành trước khi đi phỏng vấn)

Làm tay trên `minilakehouse-labs` (hoặc tạo nhanh bằng Docker: MinIO + Spark + Trino + Polaris):

- [ ] **D1 — Object storage**: `mc ls` đệ quy một prefix; chứng minh LIST chậm khi có 10K file nhỏ; viết 2 file rồi "commit thủ công" để thấy reader đọc được nửa dữ liệu (minh hoạ no-ACID).
- [ ] **D2 — Iceberg cơ bản**: CREATE TABLE `PARTITIONED BY (days(event_ts))`; INSERT; chạy query filter `event_ts` và `EXPLAIN` thấy partition pruning không cần cột partition ảo.
- [ ] **D3 — Time travel**: INSERT 3 lần; `SELECT ... FOR VERSION AS OF`; `snapshots` table; expire snapshots + rewrite_data_files (compaction) và đo số file trước/sau.
- [ ] **D4 — COW vs MOR**: UPDATE 1 nhóm row trên bảng COW — quan sát file mới viết lại; tạo bảng có position-deletes (hoặc dùng Hudi MOR nếu tiện) — so thời gian write vs read.
- [ ] **D5 — Parquet**: so query `SUM(amount) WHERE city` giữa CSV vs Parquet trên cùng dữ liệu bằng Trino/DuckDB — ghi lại bytes scanned (dùng `EXPLAIN ANALYZE`).
- [ ] **D6 — Spark skew**: DataFrame nhân bản key `'VN'` 90%; chạy `groupBy(key).count()` — xem max/median task trong UI; sửa bằng salting + AQE skew join; ghi lại con số cải thiện.
- [ ] **D7 — Shuffle/broadcast**: join fact lớn với dim nhỏ; confirm `BroadcastHashJoin` trong plan; tắt threshold để thấy `SortMergeJoin` + shuffle chênh bao nhiêu.
- [ ] **D8 — Small files**: ghi 1,000 file Parquet nhỏ; đo thời gian list + count; compact về 10 file; đo lại.
- [ ] **D9 — Semantics**: consumer Python đọc Kafka (hoặc webhook giả lập RFID): crash giữa chừng, cho thấy duplicate khi at-least-once; sửa bằng upsert theo key tại sink (bảng Iceberg/DuckDB) — nói được vì sao giờ exactly-once.
- [ ] **D10 — Diễn giải**: với mỗi drill, viết 2 câu: "kết quả đo được" + "nếu interviewer hỏi vì sao".

## 4. Khung trả lời mọi câu "design" (30 giây mở đầu)

```
1. Clarify: volume? latency SLA? nguồn? người dùng?
2. Ràng buộc: chi phí, team skill, hệ thống sẵn có
3. Đề xuất: kiến trúc + VÌ SAO (đối chiếu trade-off đã học)
4. Điểm rơi rủi ro: exactly-once? skew? small files? NÊU TRƯỚC interviewer hỏi
5. Neo trải nghiệm: "ở dự án X tôi đã gặp tương tự và xử như..."
```

Ví dụ nhanh — "Thiết kế pipeline cho 500K rows/ngày từ 12 nguồn":
> batch đủ (latency phút–giờ), Dagster SDA làm lineage, ingest dlt/Airbyte, model dbt theo medallion, storage Parquet cho analytic, nếu nguồn có CDC/RFID thì nhánh event-driven idempotent — chính là kiến trúc tôi chạy thật ở Jemmia 2 năm.

## 5. Red flags cần tránh khi trả lời

- Ném danh sách công nghệ khi chưa được hỏi (chỉ nói khi gắn được chuyện thật).
- Nói "exactly-once" mà không nhắc điều kiện ở sink.
- Chọn streaming cho mọi bài toán; Senior được đánh giá qua khả năng **từ chối** phức tạp không cần thiết.
- Không có con số: luôn kèm magnitude (rows/day, dung lượng, thời gian job trước/sau tối ưu).

*[← Part 4](part-4-spark-optimization.md) · [README](README.md)*
